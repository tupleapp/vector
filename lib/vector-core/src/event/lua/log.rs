use mlua::prelude::*;

use crate::event::{EventMetadata, LogEvent, Value};

impl<'a> ToLua<'a> for LogEvent {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn to_lua(self, lua: &'a Lua) -> LuaResult<LuaValue> {
        let (value, _metadata) = self.into_parts();
        value.to_lua(lua)
    }
}

impl<'a> FromLua<'a> for LogEvent {
    fn from_lua(lua_value: LuaValue<'a>, lua: &'a Lua) -> LuaResult<Self> {
        let value = Value::from_lua(lua_value, lua)?;
        Ok(LogEvent::from_parts(value, EventMetadata::default()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::Event;

    #[test]
    fn to_lua() {
        let mut event = Event::new_empty_log();
        let log = event.as_mut_log();
        log.insert("a", 1);
        log.insert("nested.field", "2");
        log.insert("nested.array[0]", "example value");
        log.insert("nested.array[2]", "another value");

        let assertions = vec![
            "type(log) == 'table'",
            "log.a == 1",
            "type(log.nested) == 'table'",
            "log.nested.field == '2'",
            "#log.nested.array == 3",
            "log.nested.array[1] == 'example value'",
            "log.nested.array[2] == ''",
            "log.nested.array[3] == 'another value'",
        ];

        let lua = Lua::new();
        lua.globals().set("log", log.clone()).unwrap();
        for assertion in assertions {
            let result: bool = lua
                .load(assertion)
                .eval()
                .unwrap_or_else(|_| panic!("Failed to verify assertion {:?}", assertion));
            assert!(result, "{}", assertion);
        }
    }

    #[test]
    fn from_lua() {
        let lua_event = r#"
        {
            a = 1,
            nested = {
                field = '2',
                array = {'example value', '', 'another value'}
            }
        }
        "#;

        let event: LogEvent = Lua::new().load(lua_event).eval().unwrap();

        assert_eq!(event["a"], Value::Integer(1));
        assert_eq!(event["nested.field"], Value::Bytes("2".into()));
        assert_eq!(
            event["nested.array[0]"],
            Value::Bytes("example value".into())
        );
        assert_eq!(event["nested.array[1]"], Value::Bytes("".into()));
        assert_eq!(
            event["nested.array[2]"],
            Value::Bytes("another value".into())
        );
    }
}
