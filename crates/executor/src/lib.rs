/// Query executor: evaluates physical plans against storage.
///
/// This module implements a minimal Volcano-style iterator model with
/// `Scan`, `Filter`, and `Project` operators.
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
}

pub type Row = Vec<Value>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorError {
    message: String,
}

impl ExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for ExecutorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for ExecutorError {}

pub type ExecResult<T> = Result<T, ExecutorError>;

pub trait Operator {
    fn open(&mut self) -> ExecResult<()>;
    fn next(&mut self) -> ExecResult<Option<Row>>;
    fn close(&mut self) -> ExecResult<()>;
}

pub type Predicate = Arc<dyn Fn(&Row) -> ExecResult<bool> + Send + Sync + 'static>;
pub type Projection = Arc<dyn Fn(&Row) -> ExecResult<Row> + Send + Sync + 'static>;

pub struct Scan {
    rows: Vec<Row>,
    cursor: usize,
    is_open: bool,
}

impl Scan {
    pub fn new(rows: Vec<Row>) -> Self {
        Self {
            rows,
            cursor: 0,
            is_open: false,
        }
    }
}

impl Operator for Scan {
    fn open(&mut self) -> ExecResult<()> {
        self.cursor = 0;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        if self.cursor >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(row))
    }

    fn close(&mut self) -> ExecResult<()> {
        self.is_open = false;
        self.cursor = 0;
        Ok(())
    }
}

pub struct Filter {
    input: Box<dyn Operator>,
    predicate: Predicate,
    is_open: bool,
}

impl Filter {
    pub fn new<F>(input: Box<dyn Operator>, predicate: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<bool> + Send + Sync + 'static,
    {
        Self {
            input,
            predicate: Arc::new(predicate),
            is_open: false,
        }
    }
}

impl Operator for Filter {
    fn open(&mut self) -> ExecResult<()> {
        self.input.open()?;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        loop {
            let Some(row) = self.input.next()? else {
                return Ok(None);
            };
            if (self.predicate)(&row)? {
                return Ok(Some(row));
            }
        }
    }

    fn close(&mut self) -> ExecResult<()> {
        if self.is_open {
            self.input.close()?;
        }
        self.is_open = false;
        Ok(())
    }
}

pub struct Project {
    input: Box<dyn Operator>,
    projection: Projection,
    is_open: bool,
}

impl Project {
    pub fn new<F>(input: Box<dyn Operator>, projection: F) -> Self
    where
        F: Fn(&Row) -> ExecResult<Row> + Send + Sync + 'static,
    {
        Self {
            input,
            projection: Arc::new(projection),
            is_open: false,
        }
    }
}

impl Operator for Project {
    fn open(&mut self) -> ExecResult<()> {
        self.input.open()?;
        self.is_open = true;
        Ok(())
    }

    fn next(&mut self) -> ExecResult<Option<Row>> {
        if !self.is_open {
            return Err(ExecutorError::new("operator is not open"));
        }
        let Some(row) = self.input.next()? else {
            return Ok(None);
        };
        Ok(Some((self.projection)(&row)?))
    }

    fn close(&mut self) -> ExecResult<()> {
        if self.is_open {
            self.input.close()?;
        }
        self.is_open = false;
        Ok(())
    }
}

pub fn execute(mut root: Box<dyn Operator>) -> ExecResult<Vec<Row>> {
    root.open()?;
    let mut rows = Vec::new();
    while let Some(row) = root.next()? {
        rows.push(row);
    }
    root.close()?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(v: i64) -> Value {
        Value::Integer(v)
    }

    #[test]
    fn scan_emits_rows_in_order() {
        let mut scan = Scan::new(vec![vec![int(1)], vec![int(2)]]);
        scan.open().unwrap();
        assert_eq!(scan.next().unwrap(), Some(vec![int(1)]));
        assert_eq!(scan.next().unwrap(), Some(vec![int(2)]));
        assert_eq!(scan.next().unwrap(), None);
        scan.close().unwrap();
    }

    #[test]
    fn scan_next_before_open_errors() {
        let mut scan = Scan::new(vec![vec![int(1)]]);
        let err = scan.next().unwrap_err();
        assert_eq!(err.to_string(), "operator is not open");
    }

    #[test]
    fn filter_selects_only_matching_rows() {
        let scan = Scan::new(vec![vec![int(1)], vec![int(2)], vec![int(3)]]);
        let root = Filter::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(v % 2 == 1),
            _ => Ok(false),
        });

        let out = execute(Box::new(root)).unwrap();
        assert_eq!(out, vec![vec![int(1)], vec![int(3)]]);
    }

    #[test]
    fn project_transforms_rows() {
        let scan = Scan::new(vec![vec![int(2)], vec![int(4)]]);
        let root = Project::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(vec![int(v * 10)]),
            _ => Err(ExecutorError::new("expected integer")),
        });

        let out = execute(Box::new(root)).unwrap();
        assert_eq!(out, vec![vec![int(20)], vec![int(40)]]);
    }

    #[test]
    fn scan_filter_project_pipeline() {
        let scan = Scan::new(vec![
            vec![int(1), int(10)],
            vec![int(2), int(20)],
            vec![int(3), int(30)],
        ]);
        let filter = Filter::new(Box::new(scan), |row| match row[0] {
            Value::Integer(v) => Ok(v >= 2),
            _ => Ok(false),
        });
        let project = Project::new(Box::new(filter), |row| match (&row[0], &row[1]) {
            (Value::Integer(id), Value::Integer(score)) => Ok(vec![int(*id), int(*score + 1)]),
            _ => Err(ExecutorError::new("expected integer columns")),
        });

        let out = execute(Box::new(project)).unwrap();
        assert_eq!(out, vec![vec![int(2), int(21)], vec![int(3), int(31)]]);
    }

    #[test]
    fn predicate_error_is_returned() {
        let scan = Scan::new(vec![vec![int(1)]]);
        let root = Filter::new(Box::new(scan), |_row| {
            Err(ExecutorError::new("predicate failure"))
        });
        let err = execute(Box::new(root)).unwrap_err();
        assert_eq!(err.to_string(), "predicate failure");
    }
}
