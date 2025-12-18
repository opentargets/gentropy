//! A Rust library for linear algebra operations.
//!
//! This library provides basic matrix operations including:
//! - Matrix creation and initialization
//! - Matrix addition and subtraction
//! - Matrix multiplication
//! - Matrix transpose
//! - Scalar operations

/// A matrix structure that holds 2D data in row-major order.
#[derive(Debug, Clone, PartialEq)]
pub struct Matrix {
    rows: usize,
    cols: usize,
    data: Vec<f64>,
}

impl Matrix {
    /// Creates a new matrix with the given dimensions, initialized with zeros.
    ///
    /// # Arguments
    ///
    /// * `rows` - Number of rows in the matrix
    /// * `cols` - Number of columns in the matrix
    ///
    /// # Returns
    ///
    /// A new `Matrix` instance with all elements set to 0.0
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let mat = Matrix::new(2, 3);
    /// assert_eq!(mat.rows(), 2);
    /// assert_eq!(mat.cols(), 3);
    /// ```
    pub fn new(rows: usize, cols: usize) -> Self {
        Matrix {
            rows,
            cols,
            data: vec![0.0; rows * cols],
        }
    }

    /// Creates a matrix from a 2D vector.
    ///
    /// # Arguments
    ///
    /// * `data` - A 2D vector representing the matrix data
    ///
    /// # Returns
    ///
    /// A new `Matrix` instance initialized with the provided data
    ///
    /// # Panics
    ///
    /// Panics if the input data is empty or if rows have different lengths
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let mat = Matrix::from_vec(vec![
    ///     vec![1.0, 2.0, 3.0],
    ///     vec![4.0, 5.0, 6.0],
    /// ]);
    /// assert_eq!(mat.rows(), 2);
    /// assert_eq!(mat.cols(), 3);
    /// ```
    pub fn from_vec(data: Vec<Vec<f64>>) -> Self {
        assert!(!data.is_empty(), "Matrix data cannot be empty");
        assert!(!data[0].is_empty(), "Matrix rows cannot be empty");
        let rows = data.len();
        let cols = data[0].len();
        
        // Check that all rows have the same length
        for row in &data {
            assert_eq!(row.len(), cols, "All rows must have the same length");
        }

        let flat_data: Vec<f64> = data.into_iter().flatten().collect();
        
        Matrix {
            rows,
            cols,
            data: flat_data,
        }
    }

    /// Creates an identity matrix of the given size.
    ///
    /// # Arguments
    ///
    /// * `size` - The size of the square identity matrix
    ///
    /// # Returns
    ///
    /// A square identity matrix with 1s on the diagonal and 0s elsewhere
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let identity = Matrix::identity(3);
    /// assert_eq!(identity.get(0, 0), 1.0);
    /// assert_eq!(identity.get(0, 1), 0.0);
    /// ```
    pub fn identity(size: usize) -> Self {
        let mut mat = Matrix::new(size, size);
        for i in 0..size {
            mat.set(i, i, 1.0);
        }
        mat
    }

    /// Returns the number of rows in the matrix.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Returns the number of columns in the matrix.
    pub fn cols(&self) -> usize {
        self.cols
    }

    /// Gets the value at the specified row and column.
    ///
    /// # Arguments
    ///
    /// * `row` - Row index (0-based)
    /// * `col` - Column index (0-based)
    ///
    /// # Returns
    ///
    /// The value at the specified position
    ///
    /// # Panics
    ///
    /// Panics if indices are out of bounds
    pub fn get(&self, row: usize, col: usize) -> f64 {
        assert!(row < self.rows, "Row index out of bounds");
        assert!(col < self.cols, "Column index out of bounds");
        self.data[row * self.cols + col]
    }

    /// Sets the value at the specified row and column.
    ///
    /// # Arguments
    ///
    /// * `row` - Row index (0-based)
    /// * `col` - Column index (0-based)
    /// * `value` - The value to set
    ///
    /// # Panics
    ///
    /// Panics if indices are out of bounds
    pub fn set(&mut self, row: usize, col: usize, value: f64) {
        assert!(row < self.rows, "Row index out of bounds");
        assert!(col < self.cols, "Column index out of bounds");
        self.data[row * self.cols + col] = value;
    }

    /// Adds two matrices element-wise.
    ///
    /// # Arguments
    ///
    /// * `other` - The matrix to add to this matrix
    ///
    /// # Returns
    ///
    /// A new matrix containing the element-wise sum
    ///
    /// # Panics
    ///
    /// Panics if the matrices have different dimensions
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let a = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    /// let b = Matrix::from_vec(vec![vec![5.0, 6.0], vec![7.0, 8.0]]);
    /// let c = a.add(&b);
    /// assert_eq!(c.get(0, 0), 6.0);
    /// assert_eq!(c.get(1, 1), 12.0);
    /// ```
    pub fn add(&self, other: &Matrix) -> Matrix {
        assert_eq!(self.rows, other.rows, "Matrices must have the same number of rows");
        assert_eq!(self.cols, other.cols, "Matrices must have the same number of columns");

        let mut result = Matrix::new(self.rows, self.cols);
        for i in 0..self.data.len() {
            result.data[i] = self.data[i] + other.data[i];
        }
        result
    }

    /// Subtracts another matrix from this matrix element-wise.
    ///
    /// # Arguments
    ///
    /// * `other` - The matrix to subtract from this matrix
    ///
    /// # Returns
    ///
    /// A new matrix containing the element-wise difference
    ///
    /// # Panics
    ///
    /// Panics if the matrices have different dimensions
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let a = Matrix::from_vec(vec![vec![5.0, 6.0], vec![7.0, 8.0]]);
    /// let b = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    /// let c = a.subtract(&b);
    /// assert_eq!(c.get(0, 0), 4.0);
    /// assert_eq!(c.get(1, 1), 4.0);
    /// ```
    pub fn subtract(&self, other: &Matrix) -> Matrix {
        assert_eq!(self.rows, other.rows, "Matrices must have the same number of rows");
        assert_eq!(self.cols, other.cols, "Matrices must have the same number of columns");

        let mut result = Matrix::new(self.rows, self.cols);
        for i in 0..self.data.len() {
            result.data[i] = self.data[i] - other.data[i];
        }
        result
    }

    /// Multiplies this matrix by another matrix.
    ///
    /// # Arguments
    ///
    /// * `other` - The matrix to multiply with
    ///
    /// # Returns
    ///
    /// A new matrix containing the result of matrix multiplication
    ///
    /// # Panics
    ///
    /// Panics if the number of columns in this matrix doesn't equal the number of rows in the other matrix
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let a = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    /// let b = Matrix::from_vec(vec![vec![5.0, 6.0], vec![7.0, 8.0]]);
    /// let c = a.multiply(&b);
    /// assert_eq!(c.get(0, 0), 19.0);
    /// assert_eq!(c.get(0, 1), 22.0);
    /// ```
    pub fn multiply(&self, other: &Matrix) -> Matrix {
        assert_eq!(
            self.cols, other.rows,
            "Number of columns in first matrix must equal number of rows in second matrix"
        );

        let mut result = Matrix::new(self.rows, other.cols);
        
        for i in 0..self.rows {
            for j in 0..other.cols {
                let mut sum = 0.0;
                for k in 0..self.cols {
                    sum += self.get(i, k) * other.get(k, j);
                }
                result.set(i, j, sum);
            }
        }
        
        result
    }

    /// Transposes the matrix (swaps rows and columns).
    ///
    /// # Returns
    ///
    /// A new matrix that is the transpose of this matrix
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let a = Matrix::from_vec(vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]]);
    /// let b = a.transpose();
    /// assert_eq!(b.rows(), 3);
    /// assert_eq!(b.cols(), 2);
    /// assert_eq!(b.get(0, 0), 1.0);
    /// assert_eq!(b.get(1, 0), 2.0);
    /// ```
    pub fn transpose(&self) -> Matrix {
        let mut result = Matrix::new(self.cols, self.rows);
        
        for i in 0..self.rows {
            for j in 0..self.cols {
                result.set(j, i, self.get(i, j));
            }
        }
        
        result
    }

    /// Multiplies the matrix by a scalar value.
    ///
    /// # Arguments
    ///
    /// * `scalar` - The scalar value to multiply by
    ///
    /// # Returns
    ///
    /// A new matrix with all elements multiplied by the scalar
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_linalg::Matrix;
    ///
    /// let a = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    /// let b = a.scalar_multiply(2.0);
    /// assert_eq!(b.get(0, 0), 2.0);
    /// assert_eq!(b.get(1, 1), 8.0);
    /// ```
    pub fn scalar_multiply(&self, scalar: f64) -> Matrix {
        let mut result = Matrix::new(self.rows, self.cols);
        for i in 0..self.data.len() {
            result.data[i] = self.data[i] * scalar;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_matrix() {
        let mat = Matrix::new(2, 3);
        assert_eq!(mat.rows(), 2);
        assert_eq!(mat.cols(), 3);
        assert_eq!(mat.get(0, 0), 0.0);
        assert_eq!(mat.get(1, 2), 0.0);
    }

    #[test]
    fn test_from_vec() {
        let mat = Matrix::from_vec(vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
        ]);
        assert_eq!(mat.rows(), 2);
        assert_eq!(mat.cols(), 3);
        assert_eq!(mat.get(0, 0), 1.0);
        assert_eq!(mat.get(0, 2), 3.0);
        assert_eq!(mat.get(1, 1), 5.0);
    }

    #[test]
    fn test_identity() {
        let identity = Matrix::identity(3);
        assert_eq!(identity.get(0, 0), 1.0);
        assert_eq!(identity.get(1, 1), 1.0);
        assert_eq!(identity.get(2, 2), 1.0);
        assert_eq!(identity.get(0, 1), 0.0);
        assert_eq!(identity.get(1, 2), 0.0);
    }

    #[test]
    fn test_get_set() {
        let mut mat = Matrix::new(2, 2);
        mat.set(0, 0, 1.0);
        mat.set(0, 1, 2.0);
        mat.set(1, 0, 3.0);
        mat.set(1, 1, 4.0);
        
        assert_eq!(mat.get(0, 0), 1.0);
        assert_eq!(mat.get(0, 1), 2.0);
        assert_eq!(mat.get(1, 0), 3.0);
        assert_eq!(mat.get(1, 1), 4.0);
    }

    #[test]
    fn test_add() {
        let a = Matrix::from_vec(vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
        ]);
        let b = Matrix::from_vec(vec![
            vec![5.0, 6.0],
            vec![7.0, 8.0],
        ]);
        let c = a.add(&b);
        
        assert_eq!(c.get(0, 0), 6.0);
        assert_eq!(c.get(0, 1), 8.0);
        assert_eq!(c.get(1, 0), 10.0);
        assert_eq!(c.get(1, 1), 12.0);
    }

    #[test]
    fn test_subtract() {
        let a = Matrix::from_vec(vec![
            vec![5.0, 6.0],
            vec![7.0, 8.0],
        ]);
        let b = Matrix::from_vec(vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
        ]);
        let c = a.subtract(&b);
        
        assert_eq!(c.get(0, 0), 4.0);
        assert_eq!(c.get(0, 1), 4.0);
        assert_eq!(c.get(1, 0), 4.0);
        assert_eq!(c.get(1, 1), 4.0);
    }

    #[test]
    fn test_multiply() {
        let a = Matrix::from_vec(vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
        ]);
        let b = Matrix::from_vec(vec![
            vec![5.0, 6.0],
            vec![7.0, 8.0],
        ]);
        let c = a.multiply(&b);
        
        // [1, 2]   [5, 6]   [1*5 + 2*7, 1*6 + 2*8]   [19, 22]
        // [3, 4] × [7, 8] = [3*5 + 4*7, 3*6 + 4*8] = [43, 50]
        assert_eq!(c.get(0, 0), 19.0);
        assert_eq!(c.get(0, 1), 22.0);
        assert_eq!(c.get(1, 0), 43.0);
        assert_eq!(c.get(1, 1), 50.0);
    }

    #[test]
    fn test_transpose() {
        let a = Matrix::from_vec(vec![
            vec![1.0, 2.0, 3.0],
            vec![4.0, 5.0, 6.0],
        ]);
        let b = a.transpose();
        
        assert_eq!(b.rows(), 3);
        assert_eq!(b.cols(), 2);
        assert_eq!(b.get(0, 0), 1.0);
        assert_eq!(b.get(0, 1), 4.0);
        assert_eq!(b.get(1, 0), 2.0);
        assert_eq!(b.get(1, 1), 5.0);
        assert_eq!(b.get(2, 0), 3.0);
        assert_eq!(b.get(2, 1), 6.0);
    }

    #[test]
    fn test_scalar_multiply() {
        let a = Matrix::from_vec(vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
        ]);
        let b = a.scalar_multiply(2.0);
        
        assert_eq!(b.get(0, 0), 2.0);
        assert_eq!(b.get(0, 1), 4.0);
        assert_eq!(b.get(1, 0), 6.0);
        assert_eq!(b.get(1, 1), 8.0);
    }

    #[test]
    fn test_multiply_identity() {
        let a = Matrix::from_vec(vec![
            vec![1.0, 2.0],
            vec![3.0, 4.0],
        ]);
        let identity = Matrix::identity(2);
        let result = a.multiply(&identity);
        
        assert_eq!(result.get(0, 0), 1.0);
        assert_eq!(result.get(0, 1), 2.0);
        assert_eq!(result.get(1, 0), 3.0);
        assert_eq!(result.get(1, 1), 4.0);
    }

    #[test]
    #[should_panic(expected = "Row index out of bounds")]
    fn test_get_out_of_bounds_row() {
        let mat = Matrix::new(2, 2);
        mat.get(2, 0);
    }

    #[test]
    #[should_panic(expected = "Column index out of bounds")]
    fn test_get_out_of_bounds_col() {
        let mat = Matrix::new(2, 2);
        mat.get(0, 2);
    }

    #[test]
    #[should_panic(expected = "Matrices must have the same number of rows")]
    fn test_add_different_dimensions() {
        let a = Matrix::new(2, 2);
        let b = Matrix::new(3, 2);
        a.add(&b);
    }

    #[test]
    #[should_panic(expected = "Number of columns in first matrix must equal number of rows in second matrix")]
    fn test_multiply_incompatible_dimensions() {
        let a = Matrix::new(2, 3);
        let b = Matrix::new(2, 2);
        a.multiply(&b);
    }

    #[test]
    #[should_panic(expected = "Matrix rows cannot be empty")]
    fn test_from_vec_empty_row() {
        Matrix::from_vec(vec![vec![]]);
    }
}
