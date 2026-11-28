//! 边的路由模式与分发箭头（`1|N × Shared|Isolated` 完整 2×2 对称矩阵）。

use serde::{Deserialize, Serialize};

/// 边的路由与分发模式（单值/列表 × 共享/隔离作用域的完整 2×2 对称矩阵）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeMode {
    /// `->` 整份传递一个目标输入（共享作用域）。
    Direct,
    /// `->!` 整份传递一个目标输入（进入独立新子图作用域）。
    IsolatedDirect,
    /// `->*` 共享作用域展开列表。
    SharedExpand,
    /// `->+` 按列表元素建立独立子图作用域。
    IsolatedExpand,
}

impl EdgeMode {
    pub const fn is_isolated(self) -> bool {
        matches!(self, Self::IsolatedDirect | Self::IsolatedExpand)
    }

    pub const fn is_expand(self) -> bool {
        matches!(self, Self::SharedExpand | Self::IsolatedExpand)
    }
}

/// 定义整份载荷如何分发给目标，类型参数分别为载荷类型和单份目标输入类型。
pub struct Arrow<Payload, DstIn> {
    pub(super) mode: EdgeMode,
    pub(super) inputs: fn(Payload) -> Vec<DstIn>,
}

impl<Payload, DstIn> Copy for Arrow<Payload, DstIn> {}
impl<Payload, DstIn> Clone for Arrow<Payload, DstIn> {
    fn clone(&self) -> Self {
        *self
    }
}

/// `->` 整份传递载荷（包括空列表），共享作用域。
pub fn direct<T>() -> Arrow<T, T> {
    Arrow {
        mode: EdgeMode::Direct,
        inputs: |value| vec![value],
    }
}

/// `->!` 整份传递载荷，为目标子图建立独立作用域。
pub fn isolated_direct<T>() -> Arrow<T, T> {
    Arrow {
        mode: EdgeMode::IsolatedDirect,
        inputs: |value| vec![value],
    }
}

/// `->*` 将列表展开为同一目标作用域中的多份输入。
pub fn shared_expand<T>() -> Arrow<Vec<T>, T> {
    Arrow {
        mode: EdgeMode::SharedExpand,
        inputs: std::convert::identity,
    }
}

/// `->+` 将列表逐项传入目标子图，每项建立独立作用域。
pub fn isolated_expand<T>() -> Arrow<Vec<T>, T> {
    Arrow {
        mode: EdgeMode::IsolatedExpand,
        inputs: std::convert::identity,
    }
}
