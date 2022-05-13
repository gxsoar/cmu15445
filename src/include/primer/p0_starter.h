//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>
#include "common/exception.h"
// #include "common/logger.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) : rows_(rows), cols_(cols) { linear_ = new T[rows * cols]; }

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() { delete[] linear_; }
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T *[rows];
    for (int i = 0; i < rows; ++i) {
      data_[i] = Matrix<T>::linear_ + i * cols;
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override {
    int row = Matrix<T>::rows_;
    if (row > 0) {
      return row;
    }
    return 0;
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override {
    int col = Matrix<T>::cols_;
    if (col > 0) {
      return col;
    }
    return 0;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i >= Matrix<T>::rows_ || j >= Matrix<T>::cols_ || i < 0 || j < 0) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
    // throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
    auto ans = data_[i][j];
    return ans;
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i >= Matrix<T>::rows_ || j >= Matrix<T>::cols_ || i < 0 || j < 0) {
      // throw NotImplementedException("RowMatrix::SetElement() not implented.");
      throw Exception(ExceptionType::OUT_OF_RANGE, "index is out of range");
    }
    data_[i][j] = val;
    // RowMatrix[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    int n = static_cast<int>(source.size());
    int row = Matrix<T>::rows_;
    int col = Matrix<T>::cols_;
    // LOG_INFO("n = %d, row = %d, col = %d\n", n,row, col);
    if (n != row * col) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "source is incorrect size");
    }
    // throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
    for (int i = 0; i < n; ++i) {
      Matrix<T>::linear_[i] = source[i];
    }
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override { delete[] data_; }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear`
   * array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int row_a = matrixA->GetRowCount();
    int col_a = matrixA->GetColumnCount();
    int row_b = matrixB->GetRowCount();
    int col_b = matrixB->GetColumnCount();
    if ((row_a != row_b) || (col_a != col_b)) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> matrix_c(new RowMatrix<T>(row_a, row_b));
    for (int i = 0; i < row_a; ++i) {
      for (int j = 0; j < col_a; ++j) {
        // *matrixC[i][j] = *matrixA[i][j] + *matrixB[i][j];
        // matrixC->data_[i][j] = matrixA->data_[i][j] + matrixB->data_[i][j];
        auto val = matrixA->GetElement(i, j) + matrixB->GetElement(i, j);
        matrix_c->SetElement(i, j, val);
      }
    }
    return matrix_c;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the
   * result. Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    int row_a = matrixA->GetRowCount();
    int col_a = matrixA->GetColumnCount();
    int col_b = matrixB->GetColumnCount();
    if (row_a != col_b) {
      // TODO(P0): Add implementation
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    std::unique_ptr<RowMatrix<T>> matrix_c(new RowMatrix<T>(row_a, col_b));
    for (int i = 0; i < row_a; ++i) {
      int k = 0;
      while (k < col_b) {
        int sum = 0;
        for (int j = 0; j < col_a; ++j) {
          int a = matrixA->GetElement(i, j);
          int b = matrixB->GetElement(j, k);
          sum += a * b;
        }
        matrix_c->SetElement(i, k, sum);
        k++;
      }
    }
    return matrix_c;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` *
   * `matrixB` + `matrixC`). Return `nullptr` if dimensions mismatch for input
   * matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    auto matrix_d = Multiply(matrixA, matrixB);
    if (matrix_d == nullptr) {
      // TODO(P0): Add implementation
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    auto matrix_e = Add(matrix_d.get(), matrixC);
    // auto matrix_e = Add(matrix_d, matrixC);
    if (matrix_e == nullptr) {
      return std::unique_ptr<RowMatrix<T>>(nullptr);
    }
    return matrix_e;
  }
};
}  // namespace bustub
