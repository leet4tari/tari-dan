//  Copyright 2022. The Tari Project
//
//  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//  following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//  disclaimer.
//
//  2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//  following disclaimer in the documentation and/or other materials provided with the distribution.
//
//  3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//  products derived from this software without specific prior written permission.
//
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#[cfg(not(feature = "std"))]
mod no_std {
    extern crate alloc;

    pub use alloc::{boxed, format, str, string, vec};
    pub use core::{any, cmp, fmt, iter, mem, num, ops, ptr, slice, write, writeln};

    pub mod collections {
        extern crate alloc;
        pub use alloc::collections::{BTreeMap, BTreeSet};

        #[cfg(feature = "alloc")]
        pub use hashbrown::{HashMap, HashSet};
    }
}

#[cfg(not(feature = "std"))]
pub use no_std::*;

#[cfg(feature = "std")]
mod rust_std {
    pub use ::std::{
        any,
        boxed,
        cmp,
        fmt,
        format,
        io,
        iter,
        mem,
        num,
        ops,
        ptr,
        slice,
        str,
        string,
        vec,
        write,
        writeln,
    };

    pub mod collections {
        pub use ::std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
    }
}

#[cfg(feature = "std")]
pub use rust_std::*;
