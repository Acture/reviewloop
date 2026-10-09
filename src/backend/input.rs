//! What each provider accepts as a manuscript, checked before a job is enqueued and
//! again before every dispatch, so a PDF the provider would refuse never reaches it.

use crate::util::estimate_pdf_page_count;
use anyhow::{Context, Result};
use std::{fs, io::Read, path::Path};

const MIB: u64 = 1024 * 1024;
/// The PDF header may follow up to 1 KiB of leading bytes (PDF 32000-1, 7.5.2 note).
const HEADER_WINDOW: usize = 1024;

/// A provider's published limits on the manuscript it reviews.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputPolicy {
    /// Largest PDF the provider takes, in bytes.
    pub max_bytes: u64,
    /// The provider reviews only this many leading pages; `None` when it reads them all.
    pub reviewed_pages: Option<usize>,
}

/// The limits of `backend`, or `None` when it declares none.
pub fn input_policy(backend: &str) -> Option<&'static InputPolicy> {
    match backend {
        "stanford" => Some(&super::stanford::INPUT_POLICY),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputVerdict {
    /// The provider takes the PDF. `notices` say what it will not review.
    Accepted {
        estimated_pages: usize,
        notices: Vec<String>,
    },
    /// The provider would refuse the PDF; `reason` says what to fix.
    Rejected { reason: String },
}

impl InputPolicy {
    /// Check the PDF at `path`. Only an unreadable file is an error.
    pub fn check(&self, path: &Path) -> Result<InputVerdict> {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.display().to_string());
        let size = fs::metadata(path)
            .with_context(|| format!("failed to read {}", path.display()))?
            .len();
        if size > self.max_bytes {
            return Ok(InputVerdict::Rejected {
                reason: format!(
                    "{name} is {:.1} MiB, which exceeds the provider's {} MiB limit; \
                     compress the PDF or drop the appendix and request again",
                    size as f64 / MIB as f64,
                    self.max_bytes / MIB
                ),
            });
        }
        let mut head = Vec::with_capacity(HEADER_WINDOW);
        fs::File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?
            .take(HEADER_WINDOW as u64)
            .read_to_end(&mut head)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if !head.windows(5).any(|window| window == b"%PDF-") {
            return Ok(InputVerdict::Rejected {
                reason: format!("{name} is not a PDF (no %PDF- header in its first 1 KiB)"),
            });
        }

        let estimated_pages = estimate_pdf_page_count(path)?;
        let notices = match self.reviewed_pages {
            Some(limit) if estimated_pages > limit => vec![format!(
                "the provider reviews only the first {limit} pages; {name} has an estimated \
                 {estimated_pages} pages, so pages {} onward (often the appendix) are not reviewed",
                limit + 1
            )],
            _ => Vec::new(),
        };
        Ok(InputVerdict::Accepted {
            estimated_pages,
            notices,
        })
    }
}

/// The file name to upload `path` under: its own name, with `.pdf` appended when the
/// name lacks it, since the provider's form only offers `.pdf` files.
pub fn upload_file_name(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    Some(if name.to_ascii_lowercase().ends_with(".pdf") {
        name.to_string()
    } else {
        format!("{name}.pdf")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: InputPolicy = InputPolicy {
        max_bytes: 64,
        reviewed_pages: Some(2),
    };

    fn check(bytes: &[u8]) -> InputVerdict {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("paper.pdf");
        fs::write(&path, bytes).expect("write pdf");
        POLICY.check(&path).expect("check")
    }

    #[test]
    fn accepts_a_small_pdf_without_notices() {
        assert_eq!(
            check(b"%PDF-1.4\n<< /Type /Page >>\n%%EOF\n"),
            InputVerdict::Accepted {
                estimated_pages: 1,
                notices: vec![]
            }
        );
    }

    #[test]
    fn rejects_files_over_the_size_limit() {
        let mut bytes = b"%PDF-1.4\n".to_vec();
        bytes.resize(65, b' ');
        let InputVerdict::Rejected { reason } = check(&bytes) else {
            panic!("an oversized PDF must be rejected");
        };
        assert!(
            reason.contains("exceeds the provider's 0 MiB limit"),
            "{reason}"
        );
    }

    #[test]
    fn rejects_bytes_without_a_pdf_header() {
        let InputVerdict::Rejected { reason } = check(b"<html></html>") else {
            panic!("HTML must be rejected");
        };
        assert!(reason.contains("not a PDF"), "{reason}");
        assert!(matches!(check(b""), InputVerdict::Rejected { .. }));
    }

    #[test]
    fn accepts_a_header_after_leading_bytes() {
        assert!(matches!(
            check(b"\xef\xbb\xbf%PDF-1.7\n%%EOF\n"),
            InputVerdict::Accepted { .. }
        ));
    }

    #[test]
    fn notes_pages_beyond_the_reviewed_range() {
        let InputVerdict::Accepted {
            estimated_pages,
            notices,
        } = check(b"%PDF-1.4\n<</Type/Page>><</Type/Page>><</Type/Page>>")
        else {
            panic!("a long PDF is still accepted");
        };
        assert_eq!(estimated_pages, 3);
        assert_eq!(notices.len(), 1);
        assert!(notices[0].contains("first 2 pages"), "{}", notices[0]);
        assert!(notices[0].contains("pages 3 onward"), "{}", notices[0]);
    }

    #[test]
    fn upload_file_name_keeps_pdf_names_and_adds_the_extension_otherwise() {
        assert_eq!(
            upload_file_name(Path::new("/s/abc/paper.pdf")).as_deref(),
            Some("paper.pdf")
        );
        assert_eq!(
            upload_file_name(Path::new("/s/abc/Paper.PDF")).as_deref(),
            Some("Paper.PDF")
        );
        assert_eq!(
            upload_file_name(Path::new("/s/abc/draft")).as_deref(),
            Some("draft.pdf")
        );
    }
}
