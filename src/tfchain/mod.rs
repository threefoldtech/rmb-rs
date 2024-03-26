#[subxt::subxt(runtime_metadata_path = "artifacts/network.scale")]
mod tfchain {}

use subxt::utils::AccountId32;
pub use tfchain::runtime_types::pallet_tfgrid::types::Twin as TwinData;
pub type Twin = TwinData<AccountId32>;
