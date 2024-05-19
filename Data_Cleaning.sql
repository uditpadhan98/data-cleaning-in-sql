-- Data Cleaning

SELECT *
FROM layoffs;

-- 1.Remove Dulpicates
-- 2.Standardize the data
-- 3.Null Values or blank values
-- 4.Remove any columns

-- creating a new table for all operation because doing manupulation on the main data may be risky

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- insert data from old table to new table

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- creatin a CTE to identify the dublicates rows

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

SELECT *
FROM layoffs_staging
WHERE company='Casper';

-- creatin a new table for these dublicates rows

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

-- insert the dublicates rows to the table

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2
WHERE row_num>1;


-- delete the dublicates rows

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

-- standardizing data

-- remove space from front
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- update the removed space data
UPDATE layoffs_staging2
SET company=TRIM(company);

-- renaming similar name to one common name

SELECT DISTINCT industry
from layoffs_staging2
ORDER BY 1;

SELECT distinct industry 
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2 
set industry='Crypto'
where industry like 'Crypto%';

-- renaming similar country to one common country

SELECT DISTINCT country
from layoffs_staging2
ORDER BY 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
ORDER BY 1;


update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

-- date is standiardize
select `date`,
str_to_date(`date` , '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=str_to_date(`date` , '%m/%d/%Y');

-- modify date from text data structure to Date data structure
select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` Date;

select *
from layoffs_staging2;

-- removing null values 

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company ='Airbnb';

-- null value is replace with other valuesby join method

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
    and t1.location=t2.location
where (t1.industry is null or t1.industry='')
and t2.industry is not null;


update layoffs_staging2
set industry = null
where industry='';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;


-- removing columns

-- deleting those data whose values are null because we do not find any use of these data

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select*
from layoffs_staging2;

-- delete row_num column which is create earlier to find dublicates values

alter table layoffs_staging2
drop column row_num; 