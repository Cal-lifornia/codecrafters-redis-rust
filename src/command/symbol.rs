pub struct SymbolPlus;
pub struct SymbolMinus;
pub struct SymbolWildCard;

// impl crate::redis_stream::ParseStream for SymbolMinus {
//     fn parse_stream(
//         stream: &mut crate::redis_stream::RedisStream,
//     ) -> Result<Self, crate::redis_stream::StreamParseError> {
//         let value = stream.parse::<bytes::Bytes>()?;
//         let disp = "-";
//         if *value != *disp.as_bytes() {
//             Err(crate::redis_stream::StreamParseError::Expected(
//                 "-".into(),
//                 String::from_utf8(value.to_vec()).expect("valid utf-8"),
//             ))
//         } else {
//             Ok(Self)
//         }
//     }
// }

pub mod macros {
    macro_rules! Symbol{
    ["+"] => { $crate::command::SymbolPlus };
    ["-"] => { $crate::command::SymbolMinus };
    ["*"] => { $crate::command::SymbolWildCard };
}
    pub(crate) use Symbol;
}

macro_rules! symbol_parse {
    ($symbol:ty,$str:expr) => {
        impl crate::redis_stream::ParseStream for $symbol {
            fn parse_stream(
                stream: &mut crate::redis_stream::RedisStream,
            ) -> Result<Self, crate::redis_stream::StreamParseError> {
                let value = stream.parse::<bytes::Bytes>()?;
                if *value != *$str.as_bytes() {
                    Err(crate::redis_stream::StreamParseError::Expected(
                        $str.into(),
                        String::from_utf8(value.to_vec()).expect("valid utf-8"),
                    ))
                } else {
                    Ok(Self)
                }
            }
        }
    };
}

symbol_parse!(SymbolPlus, "+");
symbol_parse!(SymbolMinus, "-");
symbol_parse!(SymbolWildCard, "*");
