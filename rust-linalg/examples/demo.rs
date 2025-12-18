use rust_linalg::Matrix;

fn main() {
    println!("=== Rust Linear Algebra Library Demo ===\n");

    // Create matrices
    println!("Creating matrices...");
    let a = Matrix::from_vec(vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
    ]);
    println!("Matrix A (2x3):");
    print_matrix(&a);

    let b = Matrix::from_vec(vec![
        vec![7.0, 8.0],
        vec![9.0, 10.0],
        vec![11.0, 12.0],
    ]);
    println!("\nMatrix B (3x2):");
    print_matrix(&b);

    // Matrix multiplication
    println!("\n--- Matrix Multiplication ---");
    let c = a.multiply(&b);
    println!("A × B:");
    print_matrix(&c);

    // Matrix addition
    println!("\n--- Matrix Addition ---");
    let d = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    let e = Matrix::from_vec(vec![vec![5.0, 6.0], vec![7.0, 8.0]]);
    println!("Matrix D:");
    print_matrix(&d);
    println!("\nMatrix E:");
    print_matrix(&e);
    
    let f = d.add(&e);
    println!("\nD + E:");
    print_matrix(&f);

    // Matrix transpose
    println!("\n--- Matrix Transpose ---");
    let g = Matrix::from_vec(vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
    ]);
    println!("Matrix G:");
    print_matrix(&g);
    
    let h = g.transpose();
    println!("\nG transposed:");
    print_matrix(&h);

    // Scalar multiplication
    println!("\n--- Scalar Multiplication ---");
    let i = Matrix::from_vec(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);
    println!("Matrix I:");
    print_matrix(&i);
    
    let j = i.scalar_multiply(3.0);
    println!("\nI × 3:");
    print_matrix(&j);

    // Identity matrix
    println!("\n--- Identity Matrix ---");
    let identity = Matrix::identity(3);
    println!("3x3 Identity Matrix:");
    print_matrix(&identity);

    println!("\n=== Demo Complete ===");
}

fn print_matrix(mat: &Matrix) {
    for i in 0..mat.rows() {
        print!("[ ");
        for j in 0..mat.cols() {
            print!("{:6.1} ", mat.get(i, j));
        }
        println!("]");
    }
}
