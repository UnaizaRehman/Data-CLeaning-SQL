-- ============================================
-- RAW DATA VIEW AND DUPLICATE REMOVAL PROCESS
-- ============================================

-- View original table
SELECT * FROM world_layoffs.layoffs;

-- Create staging table to clean data
CREATE TABLE IF NOT EXISTS world_layoffs.layoff_staging 
LIKE world_layoffs.layoffs;

-- Insert raw data into staging table
INSERT INTO world_layoffs.layoff_staging 
SELECT * FROM world_layoffs.layoffs;

-- Add row numbers to identify duplicates
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, stage, country, funds_raised_millions,
                        industry, percentage_laid_off, total_laid_off, `date`
       ) AS checks
FROM world_layoffs.layoff_staging;

-- Step 1: Create second staging table with extra column
CREATE TABLE IF NOT EXISTS world_layoffs.layoffs_staging2 (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `checks` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Step 2: Insert data with row numbers into staging2
INSERT INTO world_layoffs.layoffs_staging2
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, stage, country, funds_raised_millions,
                            industry, percentage_laid_off, total_laid_off, `date`
           ) AS checks
    FROM world_layoffs.layoff_staging
) AS sub;

-- Step 3: View duplicates
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE checks > 1;

-- Step 4: Delete duplicates
SET SQL_SAFE_UPDATES = 0;

DELETE FROM world_layoffs.layoffs_staging2
WHERE checks > 1;

-- ============================================
-- DATA STANDARDIZATION
-- ============================================

-- Trim company names
UPDATE world_layoffs.layoffs_staging2 
SET company = TRIM(company);

-- Standardize 'Crypto' industry naming
UPDATE world_layoffs.layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize country name for United States
UPDATE world_layoffs.layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%' ;

-- ============================================
-- DATE CLEANUP
-- ============================================

-- Convert date from text to DATE type
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter column type
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- ============================================
-- NULL VALUE HANDLING AND IMPUTATION
-- ============================================

-- View records with null total and percentage laid off
SELECT * 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- Standardize NULL values in industry (replace blanks with NULL)
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = ' ';

-- Fill in missing industries based on other rows of same company
UPDATE world_layoffs.layoffs_staging2 s1 
JOIN world_layoffs.layoffs_staging2 s2 
    ON s1.company = s2.company
SET s1.industry = s2.industry
WHERE s1.industry IS NULL 
  AND s2.industry IS NOT NULL;

-- Delete rows with no layoff information
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;

-- ============================================
-- FINAL CLEANUP
-- ============================================

-- Drop helper column used for deduplication
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN checks;
