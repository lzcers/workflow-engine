//! Syntax tree for typed workflow macro expansion.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Attribute, Expr, Ident, Result, Token, braced, parenthesized,
    parse::{Parse, ParseStream},
};

mod keyword {
    syn::custom_keyword!(workflow_id);
    syn::custom_keyword!(title);
    syn::custom_keyword!(context);
    syn::custom_keyword!(nodes);
    syn::custom_keyword!(edges);
    syn::custom_keyword!(subgraph);
    syn::custom_keyword!(on_error);
    syn::custom_keyword!(when);
    syn::custom_keyword!(when_error);
    syn::custom_keyword!(optional);
}

pub struct Workflow {
    pub workflow_id: Expr,
    pub title: Expr,
    pub context: Expr,
    pub region: Region,
}

#[derive(Default)]
pub struct Region {
    pub items: Vec<RegionItem>,
    pub edges: Vec<Edge>,
}

/// A declaration in source order.
pub enum RegionItem {
    Node(Node),
    Subgraph(Subgraph),
}

pub struct Node {
    pub name: Ident,
    pub instance: Expr,
    pub attributes: Vec<Attribute>,
}

pub struct Subgraph {
    pub name: Ident,
    pub attributes: Vec<Attribute>,
    pub region: Region,
}

pub struct NodePath {
    pub segments: Vec<Ident>,
}

impl NodePath {
    pub fn dotted(&self) -> String {
        self.segments
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(".")
    }
}

pub struct Edge {
    pub source: NodePath,
    pub arrow: Arrow,
    pub target: NodePath,
    pub slot: Option<Ident>,
    pub route: Route,
    pub optional: bool,
    pub transform: Option<Expr>,
}

pub enum Arrow {
    Direct,
    SharedExpand,
    IsolatedExpand,
}

pub enum Route {
    Success(Option<Expr>),
    Failure(Option<Expr>),
}

impl Parse for Workflow {
    fn parse(input: ParseStream) -> Result<Self> {
        input.parse::<keyword::workflow_id>()?;
        input.parse::<Token![:]>()?;
        let workflow_id = input.parse()?;
        input.parse::<Token![;]>()?;
        input.parse::<keyword::title>()?;
        input.parse::<Token![:]>()?;
        let title = input.parse()?;
        input.parse::<Token![;]>()?;
        input.parse::<keyword::context>()?;
        input.parse::<Token![:]>()?;
        let context = input.parse()?;
        input.parse::<Token![;]>()?;
        Ok(Self {
            workflow_id,
            title,
            context,
            region: input.parse()?,
        })
    }
}

impl Parse for Region {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut region = Self::default();
        while !input.is_empty() {
            if input.peek(keyword::nodes) {
                input.parse::<keyword::nodes>()?;
                let body;
                braced!(body in input);
                while !body.is_empty() {
                    let attributes = body.call(Attribute::parse_outer)?;
                    let name = body.parse()?;
                    body.parse::<Token![=>]>()?;
                    let instance = body.parse()?;
                    region.items.push(RegionItem::Node(Node {
                        name,
                        instance,
                        attributes,
                    }));
                    if !body.is_empty() {
                        body.parse::<Token![,]>()?;
                    }
                }
            } else if input.peek(keyword::subgraph) || input.peek(Token![#]) {
                let attributes = input.call(Attribute::parse_outer)?;
                input.parse::<keyword::subgraph>()?;
                let name = input.parse()?;
                let body;
                braced!(body in input);
                region.items.push(RegionItem::Subgraph(Subgraph {
                    name,
                    attributes,
                    region: body.parse()?,
                }));
            } else if input.peek(keyword::edges) {
                input.parse::<keyword::edges>()?;
                let body;
                braced!(body in input);
                while !body.is_empty() {
                    region.edges.push(body.parse()?);
                    if !body.is_empty() {
                        body.parse::<Token![,]>()?;
                    }
                }
            } else {
                return Err(input.error("expected nodes, subgraph, or edges"));
            }
        }
        Ok(region)
    }
}

impl Parse for NodePath {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut segments = vec![input.parse()?];
        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            segments.push(input.parse()?);
        }
        Ok(Self { segments })
    }
}

fn transform(input: ParseStream) -> Result<Option<Expr>> {
    if !input.peek(Token![|]) {
        return Ok(None);
    }
    input.parse::<Token![|]>()?;
    input.parse::<Token![>]>()?;
    Ok(Some(input.parse()?))
}

fn condition(input: ParseStream) -> Result<Expr> {
    let body;
    parenthesized!(body in input);
    let expr = body.parse()?;
    if !body.is_empty() {
        return Err(body.error("expected one condition expression"));
    }
    Ok(expr)
}

impl Parse for Edge {
    fn parse(input: ParseStream) -> Result<Self> {
        let source = input.parse()?;
        input.parse::<Token![->]>()?;
        let arrow = if input.peek(Token![*]) {
            input.parse::<Token![*]>()?;
            Arrow::SharedExpand
        } else if input.peek(Token![+]) {
            input.parse::<Token![+]>()?;
            Arrow::IsolatedExpand
        } else {
            Arrow::Direct
        };
        let target = input.parse()?;
        let slot = if input.peek(Token![::]) {
            input.parse::<Token![::]>()?;
            Some(input.parse()?)
        } else {
            None
        };

        let mut optional = false;
        if input.peek(keyword::optional) {
            input.parse::<keyword::optional>()?;
            optional = true;
        }
        let route = if input.peek(keyword::on_error) {
            let kw = input.parse::<keyword::on_error>()?;
            if optional {
                return Err(syn::Error::new_spanned(
                    kw,
                    "optional is only valid on success edges",
                ));
            }
            Route::Failure(None)
        } else if input.peek(keyword::when_error) {
            let kw = input.parse::<keyword::when_error>()?;
            if optional {
                return Err(syn::Error::new_spanned(
                    kw,
                    "optional is only valid on success edges",
                ));
            }
            Route::Failure(Some(condition(input)?))
        } else if input.peek(keyword::when) {
            input.parse::<keyword::when>()?;
            Route::Success(Some(condition(input)?))
        } else {
            Route::Success(None)
        };
        if input.peek(keyword::optional) {
            let kw = input.parse::<keyword::optional>()?;
            if matches!(route, Route::Failure(_)) {
                return Err(syn::Error::new_spanned(
                    kw,
                    "optional is only valid on success edges",
                ));
            }
            if optional {
                return Err(syn::Error::new_spanned(kw, "duplicate optional modifier"));
            }
            optional = true;
        }
        if optional && slot.is_none() {
            return Err(syn::Error::new_spanned(
                &target,
                "optional edges must target a JOIN slot (node::field)",
            ));
        }
        Ok(Self {
            source,
            arrow,
            target,
            slot,
            route,
            optional,
            transform: transform(input)?,
        })
    }
}

impl ToTokens for NodePath {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let segments = &self.segments;
        tokens.extend(quote! { #(#segments).* });
    }
}
