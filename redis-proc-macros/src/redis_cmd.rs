use quote::{quote, quote_spanned};
use syn::spanned::Spanned;

use crate::type_confirm;

pub struct RedisCmd {
    attrs: Vec<syn::Attribute>,
    vis: syn::Visibility,
    ident: syn::Ident,
    fields: syn::punctuated::Punctuated<syn::Field, syn::Token![,]>,
}

impl syn::parse::Parse for RedisCmd {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let attrs = input.call(syn::Attribute::parse_outer)?;
        let vis: syn::Visibility = input.parse()?;
        input.parse::<syn::Token![struct]>()?;
        let ident: syn::Ident = input.parse()?;
        let content;
        syn::braced!(content in input);
        Ok(RedisCmd {
            attrs,
            vis,
            ident,
            fields: content.parse_terminated(syn::Field::parse_named, syn::Token![,])?,
        })
    }
}

// redis_core::command::ParseStream
impl RedisCmd {
    pub fn impl_parse(&self) -> proc_macro2::TokenStream {
        let fields = self.fields.pairs().map(|pair| {
            let f = pair.value();
            let ident = &f.ident;
            quote_spanned! { f.span() =>
                #ident: crate::command::ParseStream::parse_stream(stream)?,
            }
        });

        let ident = &self.ident;

        quote! {
            impl crate::command::ParseStream for #ident {
                fn parse_stream(stream: &mut crate::command::InputStream) -> Result<Self, crate::command::StreamParseError> {
                    Ok(Self {
                        #(#fields)*
                    })
                }
            }
        }
    }

    pub fn impl_command(&self) -> syn::Result<proc_macro2::TokenStream> {
        let ident = &self.ident;
        let syntax = self.get_syntax()?;
        let name = ident.to_string().to_uppercase();
        let stream = quote! {
            impl crate::command::Command for #ident {
                fn name() -> &'static str {
                    #name
                }
                fn syntax() -> &'static str {
                    #syntax
                }
            }
        };
        Ok(stream)
    }

    fn get_syntax(&self) -> syn::Result<syn::LitStr> {
        let mut syntax = None::<syn::LitStr>;
        for attr in &self.attrs {
            if attr.path().is_ident("redis_command") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("syntax") {
                        let litstr: syn::LitStr = meta.value()?.parse()?;
                        syntax = Some(litstr);
                        Ok(())
                    } else {
                        Err(syn::Error::new(attr.span(), "expected 'syntax' attribute"))
                    }
                })?;
            }
        }
        if let Some(syntax) = syntax {
            Ok(syntax)
        } else {
            Err(syn::Error::new(
                self.ident.span(),
                "Expected 'syntax' attribute value",
            ))
        }
    }
}
