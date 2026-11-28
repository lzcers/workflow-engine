//! Typed expansion of the shared workflow syntax tree.

mod join_input;
mod parse;

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as Tokens};
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{
    Attribute, DeriveInput, Expr, Ident, Meta, Path, Result, Token,
    parse::{Parse, ParseStream},
};

use parse::{Arrow, NodePath, Region, RegionItem, Route, Workflow};

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

#[proc_macro_derive(JoinInput, attributes(join))]
pub fn derive_join_input(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    join_input::expand_join_input(input)
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

fn metadata(attributes: &[Attribute], default_title: &str) -> Result<(Expr, Expr)> {
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
        } else if value.path.is_ident("description") {
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
                let (title, description) = metadata(&node.attributes, &node.name.to_string())?;
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
                let (title, description) =
                    metadata(&subgraph.attributes, &subgraph.name.to_string())?;
                let owner = if parent.is_empty() {
                    quote! { None }
                } else {
                    quote! { Some(#parent) }
                };
                statements.extend(quote! {
                    let __workflow_builder = __workflow_builder.subgraph_with(
                        #name, #title, #description, #owner
                    );
                });
                register(&subgraph.region, &name, keys, statements)?;
            }
            RegionItem::Edges(_) => {}
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

/// 按源码声明顺序展开所有边（对称使用流式 `Edge` API）。
fn wire(
    region: &Region,
    parent: &str,
    engine: &Path,
    keys: &HashMap<String, Ident>,
    statements: &mut Tokens,
) -> Result<()> {
    for item in &region.items {
        match item {
            RegionItem::Node(_) => {}
            RegionItem::Subgraph(subgraph) => {
                wire(
                    &subgraph.region,
                    &qualify(parent, &subgraph.name.to_string()),
                    engine,
                    keys,
                    statements,
                )?;
            }
            RegionItem::Edges(edges) => {
                for edge in edges {
                    let source = resolve(&edge.source, parent, keys)?;
                    let target = resolve(&edge.target, parent, keys)?;
                    let arrow = match edge.arrow {
                        Arrow::Direct => quote! { #engine::dsl::direct() },
                        Arrow::IsolatedDirect => quote! { #engine::dsl::isolated_direct() },
                        Arrow::SharedExpand => quote! { #engine::dsl::shared_expand() },
                        Arrow::IsolatedExpand => quote! { #engine::dsl::isolated_expand() },
                    };
                    let (start, cond) = match &edge.route {
                        Route::Success(c) => (quote! { #engine::dsl::Edge::from(&#source) }, c),
                        Route::Failure(c) => (quote! { #engine::dsl::Edge::on_error(&#source) }, c),
                    };
                    let dst = match &edge.slot {
                        Some(slot) => quote! { .to_slot(&#target, #target.slots().#slot()) },
                        None => quote! { .to(&#target) },
                    };
                    let when_step = cond.as_ref().map(|c| quote! { .when(#c) });
                    let map_step = edge.transform.as_ref().map(|t| quote! { .map(#t) });
                    statements.extend(quote! {
                        let __workflow_builder = __workflow_builder.edge(
                            #start #dst #when_step #map_step .via(#arrow)
                        );
                    });
                }
            }
        }
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
