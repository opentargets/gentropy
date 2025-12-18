# Rust Linear Algebra Library

A lightweight Rust library providing basic linear algebra operations for matrix manipulation.

## Features

- **Matrix Creation**: Create matrices with custom dimensions or from existing data
- **Basic Operations**: Addition, subtraction, and multiplication of matrices
- **Transpose**: Compute the transpose of a matrix
- **Scalar Operations**: Multiply matrices by scalar values
- **Identity Matrix**: Generate identity matrices of any size

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
rust-linalg = { path = "path/to/rust-linalg" }
```

## Usage

### Creating Matrices

```rust
use rust_linalg::Matrix;

// Create a 2x3 zero matrix
let mat = Matrix::new(2, 3);

// Create a matrix from a 2D vector
let mat = Matrix::from_vec(vec![
    vec![1.0, 2.0, 3.0],
    vec![4.0, 5.0, 6.0],
]);

// Create a 3x3 identity matrix
let identity = Matrix::identity(3);
```

### Matrix Operations

```rust
use rust_linalg::Matrix;

// Matrix addition
let a = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
let b = Matrix::from_vec(vec![vec![5.0, 6.0], vec![7.0, 8.0]]);
let c = a.add(&b);

// Matrix subtraction
let d = a.subtract(&b);

// Matrix multiplication
let e = a.multiply(&b);

// Transpose
let f = a.transpose();

// Scalar multiplication
let g = a.scalar_multiply(2.0);
```

### Accessing and Modifying Elements

```rust
use rust_linalg::Matrix;

let mut mat = Matrix::new(2, 2);

// Set a value
mat.set(0, 0, 1.0);
mat.set(1, 1, 4.0);

// Get a value
let val = mat.get(0, 0); // Returns 1.0

// Get dimensions
let rows = mat.rows();
let cols = mat.cols();
```

## Building and Testing

Build the library:

```bash
cargo build
```

Run tests:

```bash
cargo test
```

Run tests with documentation:

```bash
cargo test --doc
```

Build documentation:

```bash
cargo doc --open
```

## API Documentation

The library provides a single main structure:

### `Matrix`

A matrix structure that holds 2D data in row-major order.

#### Methods

- `new(rows: usize, cols: usize) -> Matrix` - Create a zero matrix
- `from_vec(data: Vec<Vec<f64>>) -> Matrix` - Create from 2D vector
- `identity(size: usize) -> Matrix` - Create identity matrix
- `rows() -> usize` - Get number of rows
- `cols() -> usize` - Get number of columns
- `get(row: usize, col: usize) -> f64` - Get element value
- `set(row: usize, col: usize, value: f64)` - Set element value
- `add(&self, other: &Matrix) -> Matrix` - Element-wise addition
- `subtract(&self, other: &Matrix) -> Matrix` - Element-wise subtraction
- `multiply(&self, other: &Matrix) -> Matrix` - Matrix multiplication
- `transpose(&self) -> Matrix` - Transpose the matrix
- `scalar_multiply(&self, scalar: f64) -> Matrix` - Multiply by scalar

## License

Apache-2.0

## Contributing

This library is part of the Open Targets Gentropy project. For contributions and issues, please visit the main repository.
