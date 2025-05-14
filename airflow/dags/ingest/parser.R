#!/usr/bin/env Rscript

# Script to process ISD .gz files using multi-threading
# Usage: Rscript process_isd_files.R [input_directory] [num_cores]

# Load required libraries
library(isdparser)
library(parallel)

# Get command line arguments
args <- commandArgs(trailingOnly = TRUE)

# Set default values or use command line arguments
input_dir <- if (length(args) >= 1) args[1] else getwd()
num_cores <- if (length(args) >= 2) as.integer(args[2]) else 1

# Validate the number of cores
if (is.na(num_cores) || num_cores < 1) {
  num_cores <- 1
  cat("Invalid number of cores specified. Using 1 core.\n")
}

# Get list of .gz files in the input directory
gz_files <- list.files(path = input_dir, pattern = "*.gz$", full.names = TRUE)
cat(sprintf("Found %d .gz files in %s\n", length(gz_files), input_dir))

# Create output directory if it doesn't exist
output_dir <- file.path(input_dir, "output")
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Function to process a single file
process_file <- function(file_path) {
  tryCatch({
    # Parse the ISD file
    cat(sprintf("Processing file: %s\n", file_path))
    parsed_data <- isd_parse(file_path)

    # Generate output filename (replace .gz with .rds)
    output_file <- gsub("\\.gz$", ".rds", basename(file_path))
    output_path <- file.path(output_dir, output_file)

    # Save the parsed data
    saveRDS(parsed_data, file = output_path)

    return(sprintf("Successfully processed: %s", file_path))
  }, error = function(e) {
    return(sprintf("Error processing %s: %s", file_path, e$message))
  })
}

# Process files based on the number of cores
if (length(gz_files) > 0) {
  cat(sprintf("Processing files using %d cores...\n", num_cores))

  if (num_cores > 1) {
    # Multi-threading approach
    cl <- makeCluster(num_cores)
    results <- parLapply(cl, gz_files, process_file)
    stopCluster(cl)
  } else {
    # Single-threaded approach
    results <- lapply(gz_files, process_file)
  }

  # Print results
  cat("\nProcessing Results:\n")
  cat(unlist(results), sep = "\n")

  cat(sprintf("\nProcessing complete. Results saved to %s\n", output_dir))
} else {
  cat("No .gz files found in the specified directory.\n")
}