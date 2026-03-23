use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive macro that generates observable wrapper types for a struct.
///
/// For a struct `Foo`, this generates:
///
/// - `WatchableFoo` — each field wrapped in `Watchable<T>`, with per-field setters/getters
/// - `FooWatcher` — each field is a `Direct<T>` watcher, with per-field accessors
///
/// # Example
///
/// ```ignore
/// use watchable_rs::Watchable;
///
/// #[derive(Clone, Watchable)]
/// struct Config {
///     name: String,
///     count: u32,
/// }
///
/// let w = WatchableConfig::new(Config { name: "hello".into(), count: 42 });
/// let mut watcher = w.watch();
///
/// assert_eq!(watcher.name(), "hello");
/// assert_eq!(watcher.count(), 42);
///
/// w.set_name("world".into());
/// assert!(watcher.has_changed());
/// ```
#[proc_macro_derive(Watchable)]
pub fn derive_watchable(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_watchable(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn impl_watchable(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let vis = &input.vis;
    let generics = &input.generics;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "Watchable can only be derived for structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "Watchable can only be derived for structs",
            ))
        }
    };

    let watchable_name = format_ident!("Watchable{}", name);
    let watcher_name = format_ident!("{}Watcher", name);

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let field_names: Vec<_> = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap())
        .collect();
    let field_types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let field_vis: Vec<_> = fields.iter().map(|f| &f.vis).collect();

    // Generate setter method names: set_<field>
    let setter_names: Vec<_> = field_names
        .iter()
        .map(|name| format_ident!("set_{}", name))
        .collect();

    // Build where clause additions for Clone bounds
    let clone_bounds: Vec<_> = field_types
        .iter()
        .map(|ty| quote! { #ty: Clone })
        .collect();

    let expanded = quote! {
        /// Observable wrapper where each field is independently watchable.
        #vis struct #watchable_name #generics #where_clause {
            #(
                #field_vis #field_names: watchable_rs::Watchable<#field_types>,
            )*
        }

        impl #impl_generics #watchable_name #ty_generics
        where
            #(#clone_bounds,)*
        {
            /// Creates a new watchable wrapper from a value.
            pub fn new(value: #name #ty_generics) -> Self {
                Self {
                    #(
                        #field_names: watchable_rs::Watchable::new(value.#field_names),
                    )*
                }
            }

            /// Returns a snapshot of all fields as the original struct.
            pub fn get(&self) -> #name #ty_generics {
                #name {
                    #(
                        #field_names: self.#field_names.get(),
                    )*
                }
            }

            /// Sets all fields from a value.
            pub fn set(&self, value: #name #ty_generics) {
                #(
                    self.#field_names.set(value.#field_names);
                )*
            }

            /// Creates a watcher that observes all fields.
            pub fn watch(&self) -> #watcher_name #ty_generics {
                #watcher_name {
                    #(
                        #field_names: self.#field_names.watch(),
                    )*
                }
            }

            #(
                /// Sets the value of the `
                #[doc = stringify!(#field_names)]
                /// ` field.
                pub fn #setter_names(&self, value: #field_types) {
                    self.#field_names.set(value);
                }
            )*
        }

        impl #impl_generics Clone for #watchable_name #ty_generics #where_clause {
            fn clone(&self) -> Self {
                Self {
                    #(
                        #field_names: self.#field_names.clone(),
                    )*
                }
            }
        }

        /// Watcher that observes each field of the original struct independently.
        #vis struct #watcher_name #generics #where_clause {
            #(
                #field_vis #field_names: watchable_rs::Direct<#field_types>,
            )*
        }

        impl #impl_generics #watcher_name #ty_generics
        where
            #(#clone_bounds,)*
        {
            /// Returns a snapshot of all fields as the original struct.
            pub fn get(&mut self) -> #name #ty_generics {
                #name {
                    #(
                        #field_names: watchable_rs::Watcher::get(&mut self.#field_names),
                    )*
                }
            }

            /// Returns a snapshot without updating internal state.
            pub fn peek(&self) -> #name #ty_generics {
                #name {
                    #(
                        #field_names: watchable_rs::Watcher::peek(&self.#field_names).clone(),
                    )*
                }
            }

            /// Updates all watchers, returns `true` if any field changed.
            pub fn update(&mut self) -> bool {
                let mut changed = false;
                #(
                    changed |= watchable_rs::Watcher::update(&mut self.#field_names);
                )*
                changed
            }

            /// Returns `true` if any field has a pending change.
            pub fn has_changed(&self) -> bool {
                false #(|| watchable_rs::Watcher::has_changed(&self.#field_names))*
            }

            /// Returns `true` if all fields are still connected.
            pub fn is_connected(&self) -> bool {
                true #(&& watchable_rs::Watcher::is_connected(&self.#field_names))*
            }

            #(
                /// Returns the current cached value of the `
                #[doc = stringify!(#field_names)]
                /// ` field.
                pub fn #field_names(&self) -> &#field_types {
                    watchable_rs::Watcher::peek(&self.#field_names)
                }
            )*
        }

        impl #impl_generics Clone for #watcher_name #ty_generics
        where
            #(#clone_bounds,)*
        {
            fn clone(&self) -> Self {
                Self {
                    #(
                        #field_names: self.#field_names.clone(),
                    )*
                }
            }
        }
    };

    Ok(expanded)
}
