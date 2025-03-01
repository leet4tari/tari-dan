//   Copyright 2022. The Tari Project
//
//   Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
//   following conditions are met:
//
//   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
//   disclaimer.
//
//   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
//   following disclaimer in the documentation and/or other materials provided with the distribution.
//
//   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
//   products derived from this software without specific prior written permission.
//
//   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
//   INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
//   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
//   WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
//   USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::{cmp, io, io::Write};

/// Basic ASCII table implementation that is easy to put in a spreadsheet.
pub struct Table<'t, 's> {
    titles: Option<Vec<&'t str>>,
    rows: Vec<Vec<String>>,
    delim_str: &'s str,
    is_row_count_enabled: bool,
}

impl<'t> Table<'t, '_> {
    pub fn new() -> Self {
        Self {
            titles: None,
            rows: Vec::new(),
            delim_str: "|",
            is_row_count_enabled: false,
        }
    }

    pub fn set_titles(&mut self, titles: Vec<&'t str>) -> &mut Self {
        self.titles = Some(titles);
        self
    }

    pub fn enable_row_count(&mut self) -> &mut Self {
        self.is_row_count_enabled = true;
        self
    }

    pub fn add_row(&mut self, row: Vec<String>) -> &mut Self {
        self.rows.push(row);
        self
    }

    pub fn render<T: Write>(&self, out: &mut T) -> io::Result<()> {
        self.render_titles(out)?;
        out.write_all(b"\n")?;
        self.render_separator(out)?;
        if !self.rows.is_empty() {
            out.write_all(b"\n")?;
            self.render_rows(out)?;
            out.write_all(b"\n")?;
        }
        if self.is_row_count_enabled {
            out.write_all(format!("\n{} row(s)\n", self.rows.len()).as_bytes())?;
        }
        Ok(())
    }

    pub fn print_stdout(&self) {
        let mut stdout = io::stdout();
        self.render(&mut stdout).unwrap();
        stdout.flush().unwrap();
    }

    fn col_width(&self, idx: usize) -> usize {
        let title_width = self.titles.as_ref().map(|titles| titles[idx].len()).unwrap_or(0);
        let rows_width = self.rows.iter().fold(0, |max, r| {
            if idx < r.len() {
                cmp::max(max, r[idx].len())
            } else {
                max
            }
        });
        cmp::max(title_width, rows_width)
    }

    fn render_titles<T: Write>(&self, out: &mut T) -> io::Result<()> {
        if let Some(titles) = self.titles.as_ref() {
            self.render_row(titles, out)?;
        }
        Ok(())
    }

    fn render_rows<T: Write>(&self, out: &mut T) -> io::Result<()> {
        let rows_len = self.rows.len();
        for (i, row) in self.rows.iter().enumerate() {
            self.render_row(row, out)?;
            if i < rows_len - 1 {
                out.write_all(b"\n")?;
            }
        }
        Ok(())
    }

    fn render_row<T: Write, I: AsRef<[S]>, S: ToString>(&self, row: I, out: &mut T) -> io::Result<()> {
        let row_len = row.as_ref().len();
        for (i, string) in row.as_ref().iter().enumerate() {
            let s = string.to_string();
            let width = self.col_width(i);
            let pad_left = if i == 0 { "" } else { " " };
            let pad_right = " ".repeat(width - s.len() + 1);
            out.write_all(pad_left.as_bytes())?;
            out.write_all(s.as_bytes())?;
            out.write_all(pad_right.as_bytes())?;
            if i < row_len - 1 {
                out.write_all(self.delim_str.as_bytes())?;
            }
        }
        Ok(())
    }

    fn render_separator<T: Write>(&self, out: &mut T) -> io::Result<()> {
        if let Some(rows_len) = self.rows.first().map(|r| r.len()) {
            for i in 0..rows_len {
                let width = self.col_width(i);
                let pad_left = if i == 0 { "" } else { " " };
                out.write_all(pad_left.as_bytes())?;
                let sep = "-".repeat(width);
                out.write_all(sep.as_bytes())?;
                out.write_all(" ".as_bytes())?;
                if i < rows_len - 1 {
                    out.write_all(self.delim_str.as_bytes())?;
                }
            }
        }
        Ok(())
    }
}

impl Default for Table<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

#[macro_export]
macro_rules! table_row {
    ($($s:expr),*$(,)?) => {
        vec![$($s.to_string()),*]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn renders_titles() {
        let mut table = Table::new();
        table.set_titles(vec!["Hello", "World", "Bonjour", "Le", "Monde"]);
        let mut buf = io::Cursor::new(Vec::new());
        table.render(&mut buf).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&buf.into_inner()),
            "Hello | World | Bonjour | Le | Monde \n"
        );
    }

    #[test]
    fn renders_rows_with_titles() {
        let mut table = Table::new();
        table.set_titles(vec!["Name", "Age", "Telephone Number", "Favourite Headwear"]);
        table.add_row(table_row!["Trevor", 132, "+123 12323223", "Pith Helmet"]);
        table.add_row(table_row![]);
        table.add_row(table_row!["Hatless", 2]);
        let mut buf = io::Cursor::new(Vec::new());
        table.render(&mut buf).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&buf.into_inner()),
            "Name    | Age | Telephone Number | Favourite Headwear \n------- | --- | ---------------- | \
             ------------------ \nTrevor  | 132 | +123 12323223    | Pith Helmet        \n\nHatless | 2   \n"
        );
    }
}
