use std::num::NonZeroU64;
use std::time::Duration;

use axum::BoxError;
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use ureq::Request;

pub fn request_pulls(page_size: usize, auth_key: String, gacha_type: u8) -> BoxStream<'static, Result<GachaPull, BoxError>> {
    request_pages(
        page_size,
        move |end_id| request_pulls_page(&auth_key, page_size, end_id, gacha_type),
        |resp: PullHistoryResponse| {
            let tz = FixedOffset::east_opt(resp.data.region_time_zone * 3600).unwrap();
            resp.data.list.into_iter().map(|pull| GachaPull {
                id: Some(NonZeroU64::new(pull.id).expect("IDs are non-zero")),
                gacha_id: pull.gacha_id,
                item_id: pull.item_id,
                time: NaiveDateTime::parse_from_str(&pull.time, "%Y-%m-%d %H:%M:%S").expect("The datetime should be of format %Y-%m-%d %H:%M:%S").and_local_timezone(tz).unwrap().to_utc(),
            }).collect::<Vec<_>>()
        },
        |pull| pull.id.expect("Each pull must have an id").get(),
    )
}

fn request_pages<ID, Response, Item, FReq, FItems, FID>(
    page_size: usize,
    f_req: FReq,
    f_items: FItems,
    fid: FID,
) -> BoxStream<'static, Result<Item, BoxError>>
where
    FReq: (Fn(&Option<ID>) -> Request) + Send + 'static,
    FItems: (Fn(Response) -> Vec<Item>) + Send + 'static,
    FID: (Fn(&Item) -> ID) + Send + 'static,
    Response: Send + DeserializeOwned + 'static,
    Item: Send + 'static,
    ID: Send + Sync + 'static,
{
    async_stream::try_stream! {
        let mut end_id = None::<ID>;
        loop {
            let req = f_req(&end_id);
            let resp = tokio::task::spawn_blocking(async move || req.call().map_err(BoxError::from))
                .await?.await?
                .into_json::<Response>()
                .map(&f_items)
                .map_err(BoxError::from)?;

            end_id = resp.last().map(&fid);
            let len = resp.len();
            for pull in resp {
                yield pull;
            }
            if len < page_size {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    .boxed()
}

fn request_jades_page(authkey: &str, page_size: usize, end_id: &Option<String>) -> Request {
    let mut req = ureq::get("https://public-operation-hkrpg-sg.hoyoverse.com/common/hkrpg_self_help_inquiry/Stellar/GetList")
        .query("authkey_ver", "1")
        .query("sign_type", "2")
        .query("auth_appid", "csc")
        .query("game_biz", "hkrpg_global")
        .query("page_id", "11")
        .query("lang", "en")
        .query("authkey", authkey)
        .query("page_size", &page_size.to_string());

    if let Some(id) = end_id {
        req = req.query("end_id", id);
    }
    req
}

fn request_pulls_page(authkey: &str, page_size: usize, end_id: &Option<u64>, gacha_type: u8) -> Request {
    let mut req = ureq::get("https://public-operation-hkrpg-sg.hoyoverse.com/common/hkrpg_gacha_record/api/getGachaLog")
        .query("authkey_ver", "1")
        .query("sign_type", "2")
        .query("auth_appid", "webview_gacha")
        .query("game_biz", "hkrpg_global")
        .query("region", "prod_official_eur")
        .query("gacha_type", gacha_type.to_string().as_str())
        .query("lang", "en")
        .query("authkey", authkey)
        // .query("page", &page.to_string())
        .query("size", &page_size.to_string());
    if let Some(id) = end_id {
        req = req.query("end_id", id.to_string().as_str());
    }
    req
}



#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GachaPull {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub id: Option<NonZeroU64>, // 0 for no id, otherwise the id
    #[serde_as(as = "DisplayFromStr")]
    pub gacha_id: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub item_id: u32,
    pub time: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestedPull {
    #[serde_as(as = "DisplayFromStr")]
    pub uid: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub gacha_id: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub gacha_type: u8,
    #[serde_as(as = "DisplayFromStr")]
    pub item_id: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub count: u8,
    pub time: String,
    pub name: String,
    pub lang: String,
    pub item_type: String,
    #[serde_as(as = "DisplayFromStr")]
    pub rank_type: u8,
    #[serde_as(as = "DisplayFromStr")]
    pub id: u64,
}

#[derive(Serialize, Deserialize)]
struct PullHistoryResponseData {
    pub list: Vec<RequestedPull>,
    pub region: String,
    pub region_time_zone: i32,
}

#[derive(Serialize, Deserialize)]
struct PullHistoryResponse {
    pub retcode: i64,
    pub message: String,
    pub data: PullHistoryResponseData,
}
