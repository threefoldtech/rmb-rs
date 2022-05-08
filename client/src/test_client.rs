use rmb_rs_client::Client;



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_twin_id() {
        let client = Client::<sp_core::ed25519::Pair>::new("wss://tfchain.dev.grid.tf:443".to_string()).unwrap();
        
        assert_eq!(55, client.get_twin_id_by_account_id("5EyHmbLydxX7hXTX7gQqftCJr2e57Z3VNtgd6uxJzZsAjcPb".to_string()).unwrap());
    }
}