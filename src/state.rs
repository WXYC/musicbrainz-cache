//! Pipeline state persistence for resume support.
//!
//! Records which pipeline steps have completed so that a failed or
//! interrupted run can resume from where it left off.

use anyhow::Context;
use std::collections::HashSet;
use std::path::Path;

/// Pipeline steps in execution order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Step {
    Schema,
    Import,
    Filter,
    Indexes,
    Analyze,
}

impl Step {
    fn as_str(self) -> &'static str {
        match self {
            Step::Schema => "schema",
            Step::Import => "import",
            Step::Filter => "filter",
            Step::Indexes => "indexes",
            Step::Analyze => "analyze",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "schema" => Some(Step::Schema),
            "import" => Some(Step::Import),
            "filter" => Some(Step::Filter),
            "indexes" => Some(Step::Indexes),
            "analyze" => Some(Step::Analyze),
            _ => None,
        }
    }
}

/// Tracks which pipeline steps have completed.
#[derive(Debug, Default)]
pub struct PipelineState {
    completed: HashSet<Step>,
}

impl PipelineState {
    /// Create a new empty state (no steps completed).
    pub fn new() -> Self {
        Self::default()
    }

    /// Load state from a file. Returns empty state if the file doesn't exist.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read state file: {}", path.display()))?;
        let mut state = Self::new();
        for line in content.lines() {
            let line = line.trim();
            if !line.is_empty() {
                if let Some(step) = Step::from_str(line) {
                    state.completed.insert(step);
                }
            }
        }
        Ok(state)
    }

    /// Save state to a file, overwriting any existing content.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let mut lines: Vec<&str> = self.completed.iter().map(|s| s.as_str()).collect();
        lines.sort();
        let content = lines.join("\n") + "\n";
        std::fs::write(path, content)
            .with_context(|| format!("Failed to write state file: {}", path.display()))?;
        Ok(())
    }

    /// Check if a step has been marked complete.
    pub fn is_complete(&self, step: Step) -> bool {
        self.completed.contains(&step)
    }

    /// Mark a step as complete.
    pub fn mark_complete(&mut self, step: Step) {
        self.completed.insert(step);
    }

    /// Clear all state (reset to empty).
    pub fn clear(&mut self) {
        self.completed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_roundtrip() {
        let steps = [
            Step::Schema,
            Step::Import,
            Step::Filter,
            Step::Indexes,
            Step::Analyze,
        ];
        for step in steps {
            assert_eq!(Step::from_str(step.as_str()), Some(step));
        }
    }

    #[test]
    fn unknown_step_returns_none() {
        assert_eq!(Step::from_str("unknown"), None);
        assert_eq!(Step::from_str(""), None);
    }
}
