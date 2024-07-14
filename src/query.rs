use hyper::{header::CONTENT_LENGTH, Body, Method, Request};
use serde::Deserialize;
use tokio::task;
use url::Url; // 用于并发任务

use crate::{
    cursor::RowBinaryCursor,
    error::{Error, Result},
    response::Response,
    row::Row,
    sql::{Bind, SqlBuilder},
    Client,
};

const MAX_QUERY_LEN_TO_USE_GET: usize = 8192;
const BUFFER_SIZE: usize = 20000; // 缓冲区大小

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
}

impl Query {
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
        }
    }

    /// 绑定值到查询中的下一个 `?`。
    ///
    /// `value` 必须实现 [`Serialize`](serde::Serialize) 或者是一个 [`Identifier`]，并且会被适当地转义。
    ///
    /// 警告：这意味着查询中不应有任何额外的 `?`，即使它们在字符串字面量中！
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// 执行查询。
    pub async fn execute(self) -> Result<()> {
        self.do_execute(false)?.finish().await
    }

    /// 执行查询，返回一个 [`RowCursor`] 以获取结果。
    ///
    /// # 示例
    ///
    /// ```
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T: Row>(mut self) -> Result<RowCursor<T>> {
        self.sql.bind_fields::<T>();
        self.sql.append(" FORMAT RowBinary");

        let response = self.do_execute(true)?;
        Ok(RowCursor(RowBinaryCursor::new(response)))
    }

    /// 执行查询并仅返回一行。
    ///
    /// 注意 `T` 必须是拥有所有权的类型。
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        match self.fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// 执行查询并返回最多一行。
    ///
    /// 注意 `T` 必须是拥有所有权的类型。
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: Row + for<'b> Deserialize<'b>,
    {
        self.fetch()?.next().await
    }

    /// 执行查询并返回所有生成的结果，收集到一个 Vec 中。
    ///
    /// 注意 `T` 必须是拥有所有权的类型。
    ///
    /// # 缓冲区的意义
    ///
    /// 在并行处理中，缓冲区用于暂存从数据库获取的行数据。通过将数据分块处理，可以提高并发性能，减少处理延迟。
    /// 这里的 `BUFFER_SIZE` 表示每个缓冲区的大小，即每次并行处理的行数。增大缓冲区大小可以减少并发任务的频率，
    /// 但同时也会增加内存使用。因此，选择合适的缓冲区大小是优化性能和资源使用的关键。
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: Row + for<'b> Deserialize<'b> + std::marker::Send + 'static,
    {
        let mut cursor = self.fetch::<T>()?;
        let mut result = Vec::new();

        // 使用缓冲区并行获取行数据，并保持顺序
        let mut buffer = Vec::with_capacity(BUFFER_SIZE);

        while let Some(row) = cursor.next().await? {
            buffer.push(row);

            if buffer.len() >= BUFFER_SIZE {
                let chunk = buffer.split_off(0);
                let chunk_result = task::spawn(async move { chunk })
                    .await
                    .map_err(Error::from)?;
                result.extend(chunk_result);
            }
        }

        // 获取缓冲区中剩余的行数据
        result.extend(buffer);

        Ok(result)
    }

    pub(crate) fn do_execute(self, read_only: bool) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        let use_post = !read_only || query.len() > MAX_QUERY_LEN_TO_USE_GET;
        let method = if use_post { Method::POST } else { Method::GET };

        let (body, content_length) = if use_post {
            if read_only {
                pairs.append_pair("readonly", "1");
            }
            let len = query.len();
            (Body::from(query), len)
        } else {
            pairs.append_pair("query", &query);
            (Body::empty(), 0)
        };

        if self.client.compression.is_lz4() {
            pairs.append_pair("compress", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder().method(method).uri(url.as_str());

        if content_length == 0 {
            builder = builder.header(CONTENT_LENGTH, "0");
        } else {
            builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        }

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.client._request(request);
        Ok(Response::new(future, self.client.compression))
    }
}

/// 一个用于发出行数据的游标。
pub struct RowCursor<T>(RowBinaryCursor<T>);

impl<T> RowCursor<T> {
    /// 发出下一行数据。
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        self.0.next().await
    }
}
