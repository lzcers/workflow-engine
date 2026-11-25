//! Typed expansion of the shared workflow syntax tree.

mod parse;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{
    Attribute, Expr, Ident, Meta, Path, Result, Token,
    parse::{Parse, ParseStream},
};

use parse::{EdgeClause, NodePath, Region, RegionItem, Workflow};

struct Input {
    engine: Path,
    workflow: Workflow,
}
impl Parse for Input {
    fn parse(input: ParseStream) -> Result<Self> {
        let engine = input.parse()?;
        input.parse::<Token![;]>()?;
        Ok(Self {
            engine,
            workflow: input.parse()?,
        })
    }
}

#[proc_macro]
pub fn expand_workflow(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as Input);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn qualify(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}.{name}")
    }
}

fn metadata(
    attributes: &[Attribute],
    default_title: &str,
    allow_description: bool,
) -> Result<(Expr, Expr)> {
    let mut title = None;
    let mut description = None;
    for attribute in attributes {
        let Meta::NameValue(value) = &attribute.meta else {
            return Err(syn::Error::new_spanned(
                attribute,
                "expected #[title = expression] or #[description = expression]",
            ));
        };
        let field = if value.path.is_ident("title") {
            &mut title
        } else if allow_description && value.path.is_ident("description") {
            &mut description
        } else {
            return Err(syn::Error::new_spanned(
                attribute,
                "unsupported workflow metadata",
            ));
        };
        if field.replace(value.value.clone()).is_some() {
            return Err(syn::Error::new_spanned(
                attribute,
                "duplicate workflow metadata",
            ));
        }
    }
    Ok((
        title.unwrap_or_else(|| syn::parse_quote!(#default_title)),
        description.unwrap_or_else(|| syn::parse_quote!("")),
    ))
}

fn register(
    region: &Region,
    parent: &str,
    keys: &mut HashMap<String, Ident>,
    statements: &mut Tokens,
) -> Result<()> {
    for item in &region.items {
        match item {
            RegionItem::Node(node) => {
                let name = qualify(parent, &node.name.to_string());
                let key =
                    format_ident!("__workflow_node_{}", keys.len(), span = Span::mixed_site());
                if keys.insert(name.clone(), key.clone()).is_some() {
                    return Err(syn::Error::new_spanned(&node.name, "duplicate node name"));
                }
                let (title, description) =
                    metadata(&node.attributes, &node.name.to_string(), true)?;
                let instance = &node.instance;
                let owner = if parent.is_empty() {
                    quote! { None }
                } else {
                    quote! { Some(#parent) }
                };
                statements.extend(quote! {
                    #[allow(unused_variables)]
                    let (__workflow_builder, #key) = __workflow_builder.node_in_subgraph_with_key(
                        #name, #instance, #title, #description, #owner
                    );
                });
            }
            RegionItem::Subgraph(subgraph) => {
                let name = qualify(parent, &subgraph.name.to_string());
                let (title, _) = metadata(&subgraph.attributes, &subgraph.name.to_string(), false)?;
                let owner = if parent.is_empty() {
                    quote! { None }
                } else {
                    quote! { Some(#parent) }
                };
                statements.extend(quote! {
                    let __workflow_builder = __workflow_builder.subgraph(#name, #title, #owner);
                });
                register(&subgraph.region, &name, keys, statements)?;
            }
        }
    }
    Ok(())
}

fn resolve(path: &NodePath, parent: &str, keys: &HashMap<String, Ident>) -> Result<Ident> {
    let suffix = path.dotted();
    let mut ancestor = parent;
    loop {
        if let Some(key) = keys.get(&qualify(ancestor, &suffix)) {
            return Ok(key.clone());
        }
        if ancestor.is_empty() {
            break;
        }
        ancestor = ancestor.rsplit_once('.').map_or("", |(prefix, _)| prefix);
    }
    Err(syn::Error::new_spanned(
        path,
        format!("node {suffix} is not registered in this region"),
    ))
}

fn wire(
    region: &Region,
    parent: &str,
    engine: &Path,
    keys: &HashMap<String, Ident>,
    statements: &mut Tokens,
) -> Result<()> {
    for item in &region.items {
        if let RegionItem::Subgraph(subgraph) = item {
            wire(
                &subgraph.region,
                &qualify(parent, &subgraph.name.to_string()),
                engine,
                keys,
                statements,
            )?;
        }
    }
    for edge in &region.edges {
        let source = resolve(&edge.source, parent, keys)?;
        let target = resolve(&edge.target, parent, keys)?;
        let (failure, constructor) = match &edge.clause {
            EdgeClause::Passthrough => (
                false,
                quote! { #engine::dsl::passthrough_checked(#source, #target) },
            ),
            EdgeClause::Map(transform) => (
                false,
                quote! { #engine::dsl::map_checked(#source, #target, #transform) },
            ),
            EdgeClause::Foreach => (
                false,
                quote! { #engine::dsl::foreach_checked(#source, #target) },
            ),
            EdgeClause::OnError(transform) => (
                true,
                quote! { #engine::dsl::on_error_checked(#source, #target, #transform) },
            ),
            EdgeClause::When {
                condition,
                transform,
            } => (
                false,
                match transform {
                    Some(transform) => {
                        quote! { #engine::dsl::when_map_checked(#source, #target, #condition, #transform) }
                    }
                    None => quote! { #engine::dsl::when_checked(#source, #target, #condition) },
                },
            ),
            EdgeClause::WhenError {
                condition,
                transform,
            } => (
                true,
                match transform {
                    Some(transform) => {
                        quote! { #engine::dsl::when_error_map_checked(#source, #target, #condition, #transform) }
                    }
                    None => {
                        quote! { #engine::dsl::when_error_checked(#source, #target, #condition) }
                    }
                },
            ),
        };
        let method = if failure {
            format_ident!("error_edge")
        } else {
            format_ident!("edge")
        };
        statements
            .extend(quote! { let __workflow_builder = __workflow_builder.#method(#constructor); });
    }
    Ok(())
}

fn expand(input: Input) -> Result<Tokens> {
    let Input {
        engine,
        workflow:
            Workflow {
                workflow_id,
                title,
                context,
                region,
            },
    } = input;
    let mut keys = HashMap::new();
    let mut statements = Tokens::new();
    register(&region, "", &mut keys, &mut statements)?;
    wire(&region, "", &engine, &keys, &mut statements)?;
    Ok(quote! {{
        let __workflow_builder = #engine::Graph::builder(#workflow_id, #title, #context);
        #statements
        __workflow_builder.build()
    }})
}
