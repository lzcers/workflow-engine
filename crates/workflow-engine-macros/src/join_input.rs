//! `#[derive(JoinInput)]` implementation generating typed per-field JOIN slots.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, DeriveInput, Fields, GenericArgument, Path, PathArguments, Result, Type,
    parse_quote,
};

struct FieldAttr {
    collect: bool,
    optional: bool,
}

fn parse_crate_path(attrs: &[Attribute]) -> Result<Path> {
    let mut crate_path: Option<Path> = None;
    for attr in attrs {
        if !attr.path().is_ident("join") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                if crate_path.is_some() {
                    return Err(meta.error("duplicate `crate` attribute"));
                }
                let value = meta.value()?;
                let path: Path = value.parse()?;
                crate_path = Some(path);
                Ok(())
            } else {
                Err(meta.error("expected `crate = path` on struct"))
            }
        })?;
    }
    Ok(crate_path.unwrap_or_else(|| parse_quote!(::workflow_engine)))
}

fn parse_join_attr(attrs: &[Attribute]) -> Result<FieldAttr> {
    let mut result = FieldAttr {
        collect: false,
        optional: false,
    };
    for attr in attrs {
        if !attr.path().is_ident("join") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("collect") {
                if result.collect {
                    return Err(meta.error("duplicate `collect` attribute"));
                }
                result.collect = true;
                Ok(())
            } else if meta.path.is_ident("optional") {
                if result.optional {
                    return Err(meta.error("duplicate `optional` attribute"));
                }
                result.optional = true;
                Ok(())
            } else {
                Err(meta.error("expected `collect` or `optional`"))
            }
        })?;
    }
    Ok(result)
}

fn unwrap_generic<'a>(ty: &'a Type, wrapper: &str) -> Option<&'a Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    if type_path.qself.is_some() {
        return None;
    }
    let segment = type_path.path.segments.last()?;
    if segment.ident != wrapper {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    if args.args.len() != 1 {
        return None;
    }
    match args.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

pub fn expand_join_input(input: DeriveInput) -> Result<TokenStream> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input,
            "JoinInput can only be derived for structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input,
            "JoinInput requires a struct with named fields",
        ));
    };

    let engine = parse_crate_path(&input.attrs)?;
    let ident = &input.ident;
    let vis = &input.vis;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let slots_ident = format_ident!("__JoinSlots_{}", ident);

    let mut methods = Vec::with_capacity(fields.named.len());
    let mut specs = Vec::with_capacity(fields.named.len());
    for field in &fields.named {
        let field_ident = field
            .ident
            .as_ref()
            .expect("named struct field must have an ident");
        let field_name = field_ident.to_string();
        let attr = parse_join_attr(&field.attrs)?;

        let (item_ty, slot_ctor, merge_expr, required): (&Type, TokenStream, TokenStream, bool) =
            if attr.collect {
                let Some(inner) = unwrap_generic(&field.ty, "Vec") else {
                    return Err(syn::Error::new_spanned(
                        &field.ty,
                        "#[join(collect)] field must have type Vec<T>",
                    ));
                };
                (
                    inner,
                    quote!(append),
                    quote!(#engine::graph::MergeMode::Append),
                    !attr.optional,
                )
            } else if let Some(inner) = unwrap_generic(&field.ty, "Option") {
                (inner, quote!(set), quote!(#engine::graph::MergeMode::Set), false)
            } else {
                // `#[join(optional)]` 允许用于类型别名（如 `type MaybeX = Option<X>`）或带 `#[serde(default)]` 的字段。
                (
                    &field.ty,
                    quote!(set),
                    quote!(#engine::graph::MergeMode::Set),
                    !attr.optional,
                )
            };

        methods.push(quote! {
            #[allow(dead_code, non_snake_case)]
            #vis const fn #field_ident(
                &self,
            ) -> #engine::graph::JoinSlot<#ident #ty_generics, #item_ty> {
                #engine::graph::JoinSlot::#slot_ctor(#field_name)
            }
        });
        specs.push(quote! {
            #engine::graph::SlotSpec::new::<#item_ty>(
                #field_name,
                #merge_expr,
                #required,
            )
        });
    }

    let mut join_where = where_clause
        .cloned()
        .unwrap_or_else(|| parse_quote!(where));
    for param in generics.type_params() {
        let param_ident = &param.ident;
        join_where
            .predicates
            .push(parse_quote!(#param_ident: ::serde::Serialize + ::serde::de::DeserializeOwned + 'static));
    }

    Ok(quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types, dead_code)]
        #vis struct #slots_ident #generics(
            ::core::marker::PhantomData<fn() -> #ident #ty_generics>,
        );

        impl #impl_generics ::core::marker::Copy for #slots_ident #ty_generics #where_clause {}
        impl #impl_generics ::core::clone::Clone for #slots_ident #ty_generics #where_clause {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl #impl_generics #slots_ident #ty_generics #where_clause {
            #(#methods)*
        }

        impl #impl_generics #engine::graph::JoinInput for #ident #ty_generics #join_where {
            type Slots = #slots_ident #ty_generics;

            fn slots() -> Self::Slots {
                #slots_ident(::core::marker::PhantomData)
            }

            fn slot_specs() -> ::std::vec::Vec<#engine::graph::SlotSpec> {
                ::std::vec![#(#specs),*]
            }
        }
    })
}
