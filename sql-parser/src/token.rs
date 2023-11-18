#[derive(Debug)]
pub struct Where {
    pub key: String,
    pub value: String,
}

impl Where {
    pub fn parse(code: &str) -> Self {
        let key = code[..code.find('=').unwrap()].to_owned();
        let value = code[code.find('=').unwrap() + 1..].to_owned();

        Self { key, value }
    }
}

#[derive(Debug)]
pub enum Keywords {
    Select {
        from: String,
        r#where: Option<Where>,
    },
}

#[derive(Debug)]
pub enum Token {
    Keywords(Keywords),
    String(String),
}

pub fn tokenize(code: &str) -> Result<Vec<Token>, &str> {
    let mut tokens = Vec::new();
    let mut tokens_str: Vec<&str> = code.split(' ').into_iter().collect();

    {
        let mut i = 0;
        while i < tokens_str.len() {
            if tokens_str[i] == "" {
                tokens_str.remove(i);
                i -= 1;
            }
            i += 1;
        }
    }

    for (i, token) in tokens_str.iter().enumerate() {
        if token.to_ascii_uppercase() == "SELECT" {
            if i + 2 >= tokens_str.len() || tokens_str[i + 2].to_ascii_uppercase() != "FROM" {
                return Err("Parse error: no tables specified");
            }

            let mut r#where = None;
            if i + 4 < tokens_str.len() && tokens_str[i + 4].to_ascii_uppercase() == "WHERE" {
                r#where = Some(Where::parse(tokens_str[i + 5]));
            }
            tokens.push(Token::Keywords(Keywords::Select {
                from: tokens_str[i + 3].to_owned(),
                r#where,
            }));
        }
    }
    Ok(tokens)
}
