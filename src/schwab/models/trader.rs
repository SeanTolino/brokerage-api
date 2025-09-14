use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPreferencesResponse {
    pub accounts: Vec<AccountPreference>,
    pub offers: Vec<Offer>,
    pub streamer_info: Vec<StreamerInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountPreference {
    pub account_color: String,
    pub account_number: String,
    pub auto_position_effect: bool,
    pub display_acct_id: String,
    pub lot_selection_method: String,
    pub nick_name: String,
    pub primary_account: bool,
    #[serde(rename = "type")]
    pub account_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Offer {
    #[serde(rename = "level2Permissions")]
    pub level2_permissions: bool,
    #[serde(rename = "mktDataPermission")]
    pub mkt_data_permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamerInfo {
    pub streamer_socket_url: String,
    pub schwab_client_customer_id: String,
    pub schwab_client_correl_id: String,
    pub schwab_client_channel: String,
    pub schwab_client_function_id: String,
}