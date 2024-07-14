use clickhouse::{error::Result, test, Client, Row};
use futures::stream;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
#[derive(Serialize, Deserialize, Row)]
struct SomeRow {
    no: u32,
}

/// 创建测试表。
///
/// 该函数用于创建名为 `test` 的表。
///
/// 参数:
/// - `client`: ClickHouse 客户端实例。
///
/// 返回:
/// - `Result<()>`: 异步操作的结果。
async fn make_create(client: &Client) -> Result<()> {
    client.query("CREATE TABLE test").execute().await
}

/// 选择数据。
///
/// 该函数用于从 `who cares` 表中选择数据。
///
/// 参数:
/// - `client`: ClickHouse 客户端实例。
///
/// 返回:
/// - `Result<Vec<SomeRow>>`: 包含查询结果的行向量。
async fn make_select(client: &Client) -> Result<Vec<SomeRow>> {
    client
        .query("SELECT ?fields FROM `who cares`")
        .fetch_all::<SomeRow>()
        .await
}

/// 插入数据。
///
/// 该函数用于向 `who cares` 表中插入数据。
///
/// 参数:
/// - `client`: ClickHouse 客户端实例。
/// - `data`: 要插入的 `SomeRow` 数据。
///
/// 返回:
/// - `Result<()>`: 异步操作的结果。
async fn make_insert(client: &Client, data: &[SomeRow]) -> Result<()> {
    let mut insert = client.insert("who cares")?;
    for row in data {
        insert.write(row).await?;
    }
    insert.end().await
}

/// 监视表的变更，并在数据变更时获取最新的数据。
///
/// `make_watch` 函数用于监视 `test` 表的数据变更。它通过 `watch` 查询来订阅表的变更事件，并在数据变更时获取最新的数据。
///
/// 参数:
/// - `client`: ClickHouse 客户端实例。
///
/// 返回:
/// - `Result<(u64, SomeRow)>`: 包含版本号和最新行数据的元组。
#[cfg(feature = "watch")]
async fn make_watch(client: &Client) -> Result<(u64, SomeRow)> {
    client
        .watch("SELECT max(no) no FROM test")
        .fetch_one::<SomeRow>()
        .await
}

/// 仅监视事件。
///
/// `make_watch_only_events` 函数用于监视 `test` 表的变更事件，但不获取数据。
///
/// 参数:
/// - `client`: ClickHouse 客户端实例。
///
/// 返回:
/// - `Result<u64>`: 包含事件版本号。
#[cfg(feature = "watch")]
async fn make_watch_only_events(client: &Client) -> Result<u64> {
    client
        .watch("SELECT max(no) no FROM test")
        .only_events()
        .fetch_one()
        .await
}

#[tokio::main]
async fn main() {
    let mock = test::Mock::new();
    let client = Client::default().with_url(mock.url());
    let list = vec![SomeRow { no: 1 }, SomeRow { no: 2 }];

    // 如何测试 DDL 操作。
    let recording = mock.add(test::handlers::record_ddl());
    make_create(&client).await.unwrap();
    assert!(recording.query().await.contains("CREATE TABLE"));

    // 如何测试 SELECT 操作。
    mock.add(test::handlers::provide(stream::iter(list.clone())));
    let rows = make_select(&client).await.unwrap();
    assert_eq!(rows, list);

    // 如何测试失败情况。
    mock.add(test::handlers::failure(test::status::FORBIDDEN));
    let reason = make_select(&client).await;
    assert_eq!(format!("{reason:?}"), r#"Err(BadResponse("Forbidden"))"#);

    // 如何测试 INSERT 操作。
    let recording = mock.add(test::handlers::record());
    make_insert(&client, &list).await.unwrap();
    let rows: Vec<SomeRow> = recording.collect().await;
    assert_eq!(rows, list);

    // 如何测试 WATCH 操作。
    #[cfg(feature = "watch")]
    {
        // 检查 `CREATE LIVE VIEW`（仅针对 `watch(query)` 情况）。
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch(stream::iter(
            list.into_iter().map(|row| (42, row)),
        )));
        let (version, row) = make_watch(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 42);
        assert_eq!(row, SomeRow { no: 1 });

        // `EVENTS`。
        let recording = mock.add(test::handlers::record_ddl());
        mock.add(test::handlers::watch_only_events(stream::iter(3..5)));
        let version = make_watch_only_events(&client).await.unwrap();
        assert!(recording.query().await.contains("CREATE LIVE VIEW"));
        assert_eq!(version, 3);
    }
}
