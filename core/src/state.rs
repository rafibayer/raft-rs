use std::{collections::HashMap, error::Error};

use serde::__private::from_utf8_lossy;

pub trait Storage: Send {
    fn apply_command(&mut self, command: &[u8]) -> Result<Vec<u8>, Box<dyn Error>>;
}

impl Storage for HashMap<String, String> {
    //! Command formats:
    //!     GET <KEY>
    //!     SET <KEY> <VALUE>
    fn apply_command(&mut self, command: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let command = from_utf8_lossy(command).to_string();
        let parts = command.splitn(3, ' ').collect::<Vec<&str>>();
        match parts.len() {
            2 => {
                if let Some(result) = self.get(parts[1]) {
                    return Ok(result.as_bytes().to_vec());
                }

                return Err("Key Not Found".into())
            },
            3 => {
                self.insert(parts[1].to_string(), parts[2].to_string());
                Ok(Vec::new())
            }
            _ => Err("Malformed Command".into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hashmap_state_machine() {
        let mut state = HashMap::new();
        assert_eq!(true, state.apply_command("GET X".as_bytes()).is_err());
        assert_eq!(true, state.apply_command("SET X 1".as_bytes()).is_ok());
        assert_eq!(true, state.apply_command("GET X".as_bytes()).unwrap() == "1".as_bytes().to_vec());
        assert_eq!(true, state.apply_command("SET X 2".as_bytes()).is_ok());
        assert_eq!(true, state.apply_command("GET X".as_bytes()).unwrap() == "2".as_bytes().to_vec());
        assert_eq!(true, state.apply_command("SET Y 3".as_bytes()).is_ok());
        assert_eq!(true, state.apply_command("GET Y".as_bytes()).unwrap() == "3".as_bytes().to_vec());
    }
}
