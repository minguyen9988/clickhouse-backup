package enhanced

import (
	"fmt"
	"strings"
)

// BatchDeleteError contains detailed error information for batch delete operations
type BatchDeleteError struct {
	TotalFiles     int
	FailedFiles    int
	FailedKeys     []FailedKey
	Errors         []error
	PartialSuccess bool
}

func (e *BatchDeleteError) Error() string {
	if e.PartialSuccess {
		return fmt.Sprintf("batch delete partially failed: %d of %d files failed to delete",
			e.FailedFiles, e.TotalFiles)
	}
	return fmt.Sprintf("batch delete failed: %d of %d files failed to delete",
		e.FailedFiles, e.TotalFiles)
}

// OptimizationConfigError represents configuration-related errors
type OptimizationConfigError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *OptimizationConfigError) Error() string {
	return fmt.Sprintf("configuration error for field %s (value: %v): %s",
		e.Field, e.Value, e.Message)
}

// NewBatchDeleteError creates a new BatchDeleteError from failed keys and errors
func NewBatchDeleteError(totalFiles int, failedKeys []FailedKey, errors []error) *BatchDeleteError {
	return &BatchDeleteError{
		TotalFiles:     totalFiles,
		FailedFiles:    len(failedKeys),
		FailedKeys:     failedKeys,
		Errors:         errors,
		PartialSuccess: len(failedKeys) > 0 && len(failedKeys) < totalFiles,
	}
}

// CombineErrors combines multiple errors into a single error message
func CombineErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	if len(errors) == 1 {
		return errors[0]
	}

	var messages []string
	for _, err := range errors {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}

	return fmt.Errorf("multiple errors occurred: %s", strings.Join(messages, "; "))
}

// IsRetriableError determines if an error should trigger a retry
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error types that are typically retriable
	errStr := strings.ToLower(err.Error())
	retriablePatterns := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"temporary failure",
		"service unavailable",
		"throttled",
		"rate limit",
		"too many requests",
	}

	for _, pattern := range retriablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}
