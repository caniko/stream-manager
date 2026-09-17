//! Versioned video-mask profiles and geometry validation.
//!
//! Stored profiles only. Previewing a mask never implies outgoing video
//! is being transformed. Blur/pixelation is never described as guaranteed
//! redaction. Live compositing is deferred to the ADR.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoProfile {
    pub id: String,
    pub name: String,
    pub version: u32,
    pub kind: VideoKind,
    /// Explicit audio behavior for the transformed variant.
    pub audio: AudioBehavior,
    /// Optional timing (seconds into program); None = whole program.
    pub show_from_secs: Option<u64>,
    pub show_until_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VideoKind {
    Passthrough,
    CtaBanner(CtaBanner),
    RegionMask(RegionMask),
    FullSlate(FullSlate),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtaBanner {
    pub text: String,
    /// Resolved (reviewed-revision) link; QR optional.
    pub link_url: Option<String>,
    pub with_qr: bool,
    pub placement: Placement,
    /// Fraction of frame height (0.05..0.4).
    pub size: f32,
    /// 0.0..1.0 where applicable.
    pub opacity: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionMask {
    /// Normalized frame coordinates (0.0..1.0).
    pub x: f32,
    pub y: f32,
    pub w: f32,
    pub h: f32,
    pub opacity: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullSlate {
    pub headline: String,
    pub body: String,
    pub link_url: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Placement {
    Top,
    Bottom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioBehavior {
    /// Keep program audio on the transformed variant.
    Keep,
    /// explicitly duck/mute (declared, preview-labelled).
    Duck,
    Mute,
}

#[derive(Debug, Error, PartialEq)]
pub enum VideoError {
    #[error("coordinates out of range or degenerate")]
    BadGeometry,
    #[error("banner size out of range")]
    BadSize,
    #[error("opacity out of range")]
    BadOpacity,
    #[error("safe margin violated")]
    SafeMargin,
}

fn in_unit(v: f32) -> bool {
    (0.0..=1.0).contains(&v)
}

impl RegionMask {
    /// Validate geometry incl. a 2% safe margin from frame edges.
    pub fn validate(&self) -> Result<(), VideoError> {
        if ![self.x, self.y, self.w, self.h].iter().all(|v| in_unit(*v)) {
            return Err(VideoError::BadGeometry);
        }
        if self.w <= 0.0 || self.h <= 0.0 || self.x + self.w > 1.0 || self.y + self.h > 1.0 {
            return Err(VideoError::BadGeometry);
        }
        if !(0.0..=1.0).contains(&self.opacity) {
            return Err(VideoError::BadOpacity);
        }
        let m = 0.02;
        if self.x < m || self.y < m || self.x + self.w > 1.0 - m || self.y + self.h > 1.0 - m {
            return Err(VideoError::SafeMargin);
        }
        Ok(())
    }
}

impl CtaBanner {
    pub fn validate(&self) -> Result<(), VideoError> {
        if !(0.05..=0.4).contains(&self.size) {
            return Err(VideoError::BadSize);
        }
        if !(0.0..=1.0).contains(&self.opacity) {
            return Err(VideoError::BadOpacity);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_mask_geometry() {
        let ok = RegionMask {
            x: 0.1,
            y: 0.1,
            w: 0.5,
            h: 0.3,
            opacity: 1.0,
        };
        assert!(ok.validate().is_ok());
        let edge = RegionMask {
            x: 0.0,
            y: 0.0,
            w: 0.5,
            h: 0.5,
            opacity: 1.0,
        };
        assert_eq!(edge.validate(), Err(VideoError::SafeMargin));
        let bad = RegionMask {
            x: 0.8,
            y: 0.8,
            w: 0.5,
            h: 0.5,
            opacity: 1.0,
        };
        assert_eq!(bad.validate(), Err(VideoError::BadGeometry));
    }

    #[test]
    fn banner_size_bounds() {
        let b = CtaBanner {
            text: "Watch".into(),
            link_url: None,
            with_qr: false,
            placement: Placement::Bottom,
            size: 0.12,
            opacity: 0.95,
        };
        assert!(b.validate().is_ok());
    }
}
