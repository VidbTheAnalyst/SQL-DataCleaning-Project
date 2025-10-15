-- Exploratory Data analysis Project 

SELECT 
    *
FROM
    layoffs_staging2;

SELECT 
    MAX(total_laid_off), MAX(percentage_laid_off)
FROM
    layoffs_staging2;


SELECT 
    *
FROM
    layoffs_staging2
WHERE
    percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT 
    company, SUM(total_laid_off) AS Sum_LaidOffs
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT 
    MIN(`date`), MAX(`date`)
FROM
    layoffs_staging2;

SELECT 
    country, SUM(total_laid_off) AS Total_LaidOffs
FROM
    layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT 
    *
FROM
    layoffs_staging2;

SELECT 
    YEAR(`date`) AS In_Year,
    SUM(total_laid_off) AS Total_LaidOffs
FROM
    layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT 
    stage, SUM(total_laid_off) AS Total_LaidOffs
FROM
    layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;

SELECT 
    company, SUM(percentage_laid_off) AS Percentage_Layoffs
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT 
    company, AVG(percentage_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT 
    SUBSTRING(`date`, 1, 7) AS `Month`, SUM(total_laid_off)
FROM
    layoffs_staging2
WHERE
    SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
;

with Rolling_total as
(
select substring(`date`, 1, 7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by `Month`
order by 1 asc
)
Select `Month`, total_off, sum(total_off) over(order by `Month`) as rolling_total
from Rolling_total
;

SELECT 
    company, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

SELECT 
    company,
    YEAR(`date`) AS In_Year,
    SUM(total_laid_off) AS Total_LaidOffs
FROM
    layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC
;

With company_year (company, years, total_laid_off) As
(
select company, Year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, Year(`date`)
Order by 3 Desc
), company_year_rank as 
(Select *, Dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from company_year 
Where years is not null
)
select *
from company_year_rank
Where Ranking <= 5
;

