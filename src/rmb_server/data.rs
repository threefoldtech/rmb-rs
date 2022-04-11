/// - This is an example struct which holds the shared parameters between all handlers
/// such as the database or cache
/// - You can build your own Data object or change this model
///
/// Initialization Example
///
/// ```
/// let r = RedisStorage;
/// let i = SubstrateIdentity;
///
/// let d = Data {
///     storage: &r,
///     identity: &i,
/// };
///
/// ```

#[allow(dead_code)]
pub struct Data<'a, 'b, S, I> {
    pub storage: &'b S,
    pub identity: &'a I,
}
