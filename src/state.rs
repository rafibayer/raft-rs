use std::collections::HashMap;

pub trait Storage: Sync + Send {
    fn apply_command(&mut self, command: &str) -> Result<String, String>;
}

impl Storage for HashMap<String, String> {
    
    //! Command formats: 
    //!     GET <KEY>
    //!     SET <KEY> <VALUE>
    fn apply_command(&mut self, command: &str) -> Result<String, String> {
        let parts = command.splitn(3, " ").collect::<Vec<&str>>();
        match parts.len() {
            2 => {
                self.get(parts[1]).cloned().ok_or(String::from("KEY NOT FOUND"))
            },
            3 => {
                self.insert(parts[1].to_string(), parts[2].to_string());
                Ok(String::new())
            },
            _ => Err("Malformed Command".to_string())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hashmap_state_machine() {
        let mut state = HashMap::new();
        assert_eq!(true, state.apply_command("GET X").is_err());
        assert_eq!(true, state.apply_command("SET X 1").is_ok());
        assert_eq!(true, state.apply_command("GET X") == Ok("1".to_string()));
        assert_eq!(true, state.apply_command("SET X 2").is_ok());
        assert_eq!(true, state.apply_command("GET X") == Ok("2".to_string()));
        assert_eq!(true, state.apply_command("SET Y 3").is_ok());
        assert_eq!(true, state.apply_command("GET Y") == Ok("3".to_string()));
    }

}