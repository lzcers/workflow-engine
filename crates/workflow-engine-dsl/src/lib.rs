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
    syn::custom_keyword!(foreach);
    syn::custom_keyword!(on_error);
    syn::custom_keyword!(when);
    syn::custom_keyword!(when_error);
}

#[derive(Clone)]
pub struct Workflow {
    pub workflow_id: Expr,
    pub title: Expr,
    pub context: Expr,
    pub region: Region,
}

#[derive(Clone, Default)]
pub struct Region {
    pub nodes: Vec<Node>,
    pub subgraphs: Vec<Subgraph>,
    pub edges: Vec<Edge>,
    pub order: Vec<RegionItem>,
}

/// Order of node construction and nested declarations in the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionItem {
    Node(usize),
    Subgraph(usize),
}

#[derive(Clone)]
pub struct Node {
    pub name: Ident,
    pub instance: Expr,
    pub attributes: Vec<Attribute>,
}

#[derive(Clone)]
pub struct Subgraph {
    pub name: Ident,
    pub attributes: Vec<Attribute>,
    pub region: Region,
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct Edge {
    pub source: NodePath,
    pub target: NodePath,
    pub clause: EdgeClause,
}

#[derive(Clone)]
pub enum EdgeClause {
    Passthrough,
    Map(Expr),
    Foreach,
    OnError(Expr),
    When {
        condition: Expr,
        transform: Option<Expr>,
    },
    WhenError {
        condition: Expr,
        transform: Option<Expr>,
    },
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
                    region.order.push(RegionItem::Node(region.nodes.len()));
                    region.nodes.push(Node {
                        name,
                        instance,
                        attributes,
                    });
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
                region
                    .order
                    .push(RegionItem::Subgraph(region.subgraphs.len()));
                region.subgraphs.push(Subgraph {
                    name,
                    attributes,
                    region: body.parse()?,
                });
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

impl Parse for Edge {
    fn parse(input: ParseStream) -> Result<Self> {
        let source = input.parse()?;
        input.parse::<Token![->]>()?;
        let target = input.parse()?;
        let clause = if input.peek(keyword::foreach) {
            input.parse::<keyword::foreach>()?;
            EdgeClause::Foreach
        } else if input.peek(keyword::on_error) {
            input.parse::<keyword::on_error>()?;
            EdgeClause::OnError(input.parse()?)
        } else if input.peek(keyword::when) || input.peek(keyword::when_error) {
            let is_error = input.peek(keyword::when_error);
            if is_error {
                input.parse::<keyword::when_error>()?;
            } else {
                input.parse::<keyword::when>()?;
            }
            let body;
            parenthesized!(body in input);
            let condition = body.parse()?;
            if !body.is_empty() {
                return Err(body.error("expected one condition expression"));
            }
            let transform = transform(input)?;
            if is_error {
                EdgeClause::WhenError {
                    condition,
                    transform,
                }
            } else {
                EdgeClause::When {
                    condition,
                    transform,
                }
            }
        } else if let Some(expr) = transform(input)? {
            EdgeClause::Map(expr)
        } else {
            EdgeClause::Passthrough
        };
        Ok(Self {
            source,
            target,
            clause,
        })
    }
}

impl ToTokens for Workflow {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            workflow_id,
            title,
            context,
            region,
        } = self;
        tokens.extend(
            quote! { workflow_id: #workflow_id; title: #title; context: #context; #region },
        );
    }
}
impl ToTokens for Region {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        for item in &self.order {
            match item {
                RegionItem::Node(index) => {
                    let node = &self.nodes[*index];
                    tokens.extend(quote! { nodes { #node, } });
                }
                RegionItem::Subgraph(index) => self.subgraphs[*index].to_tokens(tokens),
            }
        }
        let edges = &self.edges;
        tokens.extend(quote! { edges { #(#edges,)* } });
    }
}
impl ToTokens for Node {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            name,
            instance,
            attributes,
        } = self;
        tokens.extend(quote! { #(#attributes)* #name => #instance });
    }
}
impl ToTokens for Subgraph {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            name,
            attributes,
            region,
        } = self;
        tokens.extend(quote! { #(#attributes)* subgraph #name { #region } });
    }
}
impl ToTokens for NodePath {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let segments = &self.segments;
        tokens.extend(quote! { #(#segments).* });
    }
}
impl ToTokens for Edge {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Self {
            source,
            target,
            clause,
        } = self;
        tokens.extend(quote! { #source -> #target #clause });
    }
}
impl ToTokens for EdgeClause {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(match self {
            Self::Passthrough => quote! {},
            Self::Map(expr) => quote! { |> #expr },
            Self::Foreach => quote! { foreach },
            Self::OnError(expr) => quote! { on_error #expr },
            Self::When {
                condition,
                transform,
            } => {
                let map = transform.as_ref().map(|expr| quote! { |> #expr });
                quote! { when(#condition) #map }
            }
            Self::WhenError {
                condition,
                transform,
            } => {
                let map = transform.as_ref().map(|expr| quote! { |> #expr });
                quote! { when_error(#condition) #map }
            }
        });
    }
}
