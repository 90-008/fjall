// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::entry::Entry;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

/// Reads and emits through the entries in a journal file, but doesn't
/// check the validity of batches
///
/// Will truncate the file to the last valid position to prevent corrupt
/// bytes at the end of the file, which would jeopardize future writes into the file.
#[expect(clippy::module_name_repetitions)]
pub struct JournalReader {
    pub(crate) path: PathBuf,
    pub(crate) reader: BufReader<File>,
    pub(crate) last_valid_pos: u64,

    /// Bytes consumed so far, tracked here because `stream_position`
    /// costs an lseek syscall per entry
    pos: u64,
}

/// Counts the bytes an entry decode consumes
struct CountingReader<'a, R> {
    inner: &'a mut R,
    read: u64,
}

impl<R: Read> Read for CountingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        Ok(n)
    }
}

impl JournalReader {
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        Ok(Self {
            path: path.as_ref().into(),
            reader: BufReader::new(file),
            last_valid_pos: 0,
            pos: 0,
        })
    }

    fn truncate_file(&mut self, pos: u64) -> crate::Result<()> {
        log::debug!("truncating journal to {pos}");
        self.reader.get_mut().set_len(pos)?;
        self.reader.get_mut().sync_all()?;
        Ok(())
    }

    fn maybe_truncate_file_to_last_valid_pos(&mut self) -> crate::Result<()> {
        if self.pos > self.last_valid_pos {
            self.truncate_file(self.last_valid_pos)?;
        }

        Ok(())
    }
}

impl Iterator for JournalReader {
    type Item = crate::Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut counted = CountingReader {
            inner: &mut self.reader,
            read: 0,
        };
        let decoded = Entry::decode_from(&mut counted);
        self.pos += counted.read;

        match decoded {
            Ok(item) => {
                self.last_valid_pos = self.pos;
                Some(Ok(item))
            }
            Err(e) => {
                if let crate::Error::Io(e) = e {
                    match e.kind() {
                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::Other => {
                            fail_iter!(self.maybe_truncate_file_to_last_valid_pos());
                            None
                        }
                        _ => Some(Err(crate::Error::Io(e))),
                    }
                } else {
                    fail_iter!(self.maybe_truncate_file_to_last_valid_pos());
                    None
                }
            }
        }
    }
}
