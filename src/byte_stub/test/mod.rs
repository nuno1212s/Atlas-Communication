use strum::IntoEnumIterator;
use crate::byte_stub::MODULES;
use crate::lookup_table::MessageModule;

#[test]
fn test_module_count() {
    let messages = MessageModule::iter().count();

    assert_eq!(messages, MODULES)
}