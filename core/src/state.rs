use std::collections::HashMap;

pub trait Storage<TCommand, TResult>: Send {
    fn apply_command(&mut self, command: TCommand) -> Result<TResult, String>;
}

impl Storage<String, String> for HashMap<String, String> {
    //! Command formats:
    //!     GET <KEY>
    //!     SET <KEY> <VALUE>
    fn apply_command(&mut self, command: String) -> Result<String, String> {
        let parts = command.splitn(3, ' ').collect::<Vec<&str>>();
        match parts.len() {
            2 => self.get(parts[1]).cloned().ok_or_else(|| "KEY NOT FOUND".to_string()),
            3 => {
                self.insert(parts[1].to_string(), parts[2].to_string());
                Ok(String::new())
            }
            _ => Err("Malformed Command".to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hashmap_state_machine() {
        let mut state = HashMap::new();
        assert_eq!(true, state.apply_command(String::from("GET X")).is_err());
        assert_eq!(true, state.apply_command(String::from("SET X 1")).is_ok());
        assert_eq!(true, state.apply_command(String::from("GET X")) == Ok("1".to_string()));
        assert_eq!(true, state.apply_command(String::from("SET X 2")).is_ok());
        assert_eq!(true, state.apply_command(String::from("GET X")) == Ok("2".to_string()));
        assert_eq!(true, state.apply_command(String::from("SET Y 3")).is_ok());
        assert_eq!(true, state.apply_command(String::from("GET Y")) == Ok("3".to_string()));
    }
}
