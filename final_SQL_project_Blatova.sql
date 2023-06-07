CREATE OR REPLACE TABLE t_jaroslava_blatova_project_SQL_primary_final AS
/* base code */
WITH base_payroll AS (
	SELECT 
		id, 
		value, 
		value_type_code,
		unit_code, 
		calculation_code, 
		industry_branch_code, 
		payroll_year, 
		payroll_quarter
	FROM data_academy_02_2023.czechia_payroll
	WHERE value_type_code = 5958 
),
base_payroll_calculation AS (
	SELECT 
		code,
		name
	FROM data_academy_02_2023.czechia_payroll_calculation
),
base_payroll_industry_branch AS (
	SELECT 
		code, 
		name
	FROM data_academy_02_2023.czechia_payroll_industry_branch
),
base_payroll_unit AS (
	SELECT 
		code, 
		name
	FROM data_academy_02_2023.czechia_payroll_unit
),
/* transformation */
payroll_combined AS (
	SELECT 
		i.name AS industry_branch_name,
		p.payroll_year,
		c.name AS calculation_name,
		ROUND(AVG(p.value),0) AS avg_salary,
		u.name AS unit_name
	FROM data_academy_02_2023.czechia_payroll p
	LEFT JOIN data_academy_02_2023.czechia_payroll_calculation c
		ON p.calculation_code = c.code
	LEFT JOIN data_academy_02_2023.czechia_payroll_industry_branch i 
		ON p.industry_branch_code = i.code
	LEFT JOIN data_academy_02_2023.czechia_payroll_unit u
		ON p.unit_code = u.code
	WHERE p.value_type_code = 5958
	GROUP BY industry_branch_name, payroll_year, calculation_name
),
payroll_combined_ly AS (
	SELECT *,
		LAG(avg_salary,1) OVER (PARTITION BY industry_branch_name, calculation_name ORDER BY industry_branch_name, calculation_name, payroll_year) AS avg_salary_ly,
		(avg_salary - LAG(avg_salary,1) OVER (PARTITION BY industry_branch_name, calculation_name ORDER BY industry_branch_name, calculation_name, payroll_year)) AS avg_salary_diff,
		round( (avg_salary - LAG(avg_salary,1) OVER (PARTITION BY industry_branch_name, calculation_name ORDER BY industry_branch_name, calculation_name, payroll_year)) / (LAG(avg_salary,1) OVER (PARTITION BY industry_branch_name, calculation_name ORDER BY industry_branch_name, calculation_name, payroll_year)) * 100,2) AS avg_salary_growth_percent
	FROM payroll_combined
),
price_combined AS (
	SELECT 
		YEAR(cp.date_from) AS price_year,
		cpc.name AS food_category,
		cpc.price_value AS food_category_volume,
		cpc.price_unit AS food_category_unit,
		round(avg(cp.value),2) AS avg_price
	FROM data_academy_02_2023.czechia_price cp 
	LEFT JOIN data_academy_02_2023.czechia_price_category cpc 
		ON cp.category_code = cpc.code 
	GROUP BY price_year, food_category, food_category_volume, food_category_unit
),
price_combined_ly AS (
	SELECT
		p1.*,
		p2.avg_price AS avg_price_ly,
	    round( ( p1.avg_price - p2.avg_price ) / p2.avg_price * 100, 2) as avg_price_growth_percent
	FROM price_combined p1
	LEFT JOIN price_combined p2
		ON p1.food_category = p2.food_category 
	    AND p1.price_year = p2.price_year + 1
)
SELECT
	py.*,
	pr.*
FROM payroll_combined_ly py
LEFT JOIN price_combined_ly pr
	ON py.payroll_year = pr.price_year
	
	
CREATE OR REPLACE TABLE t_jaroslava_blatova_project_SQL_secondary_final  AS
SELECT 
	e.country,
	e.year,
	c.abbreviation,
	c.population,
	e.gini,
	e.GDP
FROM economies e 
LEFT JOIN countries c 
	ON e.country = c.country 
WHERE continent = 'Europe' 
	AND YEAR BETWEEN '2006' AND '2018';
	


/* Otázka č. 1 
 * Rostou v průběhu let mzdy ve všech odvětvích, nebo v některých klesají?*/

SELECT 
	industry_branch_name,
	payroll_year, 
	avg_salary_diff
FROM t_jaroslava_blatova_project_SQL_primary_final
WHERE avg_salary_diff < 0
GROUP BY industry_branch_name

/*Otázka č. 2
 * Kolik je možné si koupit litrů mléka a kilogramů chleba za první a poslední srovnatelné období v dostupných datech cen a mezd?*/

WITH price_years AS (
	SELECT 
		food_category,
		MIN(price_year) AS first_year,
		MAX(price_year) AS last_year 
	FROM t_jaroslava_blatova_project_SQL_primary_final
	WHERE price_year IS NOT NULL
	GROUP BY food_category
),
filtered_table AS (
	SELECT
		t1.price_year,
		t1.food_category,
		t1.food_category_volume,
		t1.food_category_unit,
		t1.avg_price,
		t1.unit_name,
		t1.calculation_name,
		ROUND(AVG(t1.avg_salary),0) AS avg_salary
	FROM t_jaroslava_blatova_project_SQL_primary_final t1
	INNER JOIN price_years py
		ON t1.food_category = py.food_category
		AND t1.price_year = py.first_year OR t1.price_year = py.last_year
	WHERE t1.food_category IN ('Mléko polotučné pasterované' , 'Chléb konzumní kmínový')
	GROUP BY 
		t1.price_year,
		t1.food_category,
		t1.food_category_volume,
		t1.food_category_unit,
		t1.avg_price,
		t1.unit_name,
		t1.calculation_name
)
SELECT
	*,
	round(avg(avg_salary)/avg_price, 0) AS buyable_amount
FROM filtered_table
WHERE calculation_name = 'přepočtený'
GROUP BY 
	price_year,
	food_category,
	food_category_volume,
	food_category_unit,
	avg_price,
	unit_name,
	calculation_name,
	avg_salary
	;
	
/*Otázka č. 3 
 * Která kategorie potravin zdražuje nejpomaleji (je u ní nejnižší percentuální meziroční nárůst)?*/

WITH price_years AS (
	SELECT 
		food_category,
		MIN(price_year) AS first_year,
		MAX(price_year) AS last_year 
	FROM t_jaroslava_blatova_project_SQL_primary_final
	WHERE price_year IS NOT NULL
	GROUP BY food_category
),
price_values AS (
	SELECT
		py.food_category,
		py.first_year,
		py.last_year,
		t1.avg_price AS avg_price_first_year,
		t2.avg_price AS avg_price_last_year
	FROM price_years py
	LEFT JOIN t_jaroslava_blatova_project_SQL_primary_final t1
		ON t1.food_category = py.food_category
		AND t1.price_year = py.first_year
	LEFT JOIN t_jaroslava_blatova_project_SQL_primary_final t2
		ON t2.food_category = py.food_category
		AND t2.price_year = py.last_year
	GROUP BY food_category
)
SELECT
	food_category,
	avg_price_first_year,
	avg_price_last_year,
	round(((avg_price_last_year - avg_price_first_year) / avg_price_first_year) * 100, 2) AS avg_price_growth_percent,
	last_year - first_year + 1 AS years_tracked,
	round(((avg_price_last_year - avg_price_first_year) / avg_price_first_year) * 100 / (last_year - first_year + 1), 2) AS avg_price_growth_percent_yearly
FROM price_values
ORDER BY avg_price_growth_percent ASC 
;

/*Otázka č.4
 * Existuje rok, ve kterém byl meziroční nárůst cen potravin výrazně vyšší než růst mezd (větší než 10 %)?*/

WITH grouped_table AS (
	SELECT 
		payroll_year,
		calculation_name,
		avg(avg_salary) AS avg_salary,
		avg(avg_salary_ly) AS avg_salary_ly,
		avg(avg_price) AS avg_price,
		avg(avg_price_ly) AS avg_price_ly
	FROM t_jaroslava_blatova_project_SQL_primary_final
	WHERE price_year IS NOT NULL
		AND avg_price_ly IS NOT NULL
		AND calculation_name = 'přepočtený' -- 'fyzický' 
	GROUP BY
		payroll_year,
		calculation_name
)
SELECT 
	payroll_year,
	calculation_name,
	ROUND(((avg_salary - avg_salary_ly) / avg_salary_ly * 100), 2) AS salary_growth_percent,
	ROUND(((avg_price - avg_price_ly) / avg_price_ly * 100), 2) AS price_growth_percent
FROM grouped_table
;

/*Otázka č.5
 * Má výše HDP vliv na změny ve mzdách a cenách potravin? Neboli, pokud HDP vzroste výrazněji v jednom roce, 
 projeví se to na cenách potravin či mzdách ve stejném nebo násdujícím roce výraznějším růstem?*/

WITH main_table AS (
	SELECT 
		payroll_year,
		calculation_name,
		avg(avg_salary) AS avg_salary,
		avg(avg_salary_ly) AS avg_salary_ly,
		avg(avg_price) AS avg_price,
		avg(avg_price_ly) AS avg_price_ly
	FROM t_jaroslava_blatova_project_SQL_primary_final
	WHERE price_year IS NOT NULL
		AND avg_price_ly IS NOT NULL
		AND calculation_name = 'přepočtený' -- 'fyzický' 
	GROUP BY
		payroll_year,
		calculation_name
),
main_table_growths AS (
SELECT 
	payroll_year,
	calculation_name,
	ROUND(((avg_salary - avg_salary_ly) / avg_salary_ly * 100), 2) AS salary_growth_percent,
	ROUND(((avg_price - avg_price_ly) / avg_price_ly * 100), 2) AS price_growth_percent
FROM main_table
),
hdp_table AS (
	SELECT
		year,
		GDP
	FROM t_jaroslava_blatova_project_SQL_secondary_final
	WHERE LOWER(country) = 'czech republic'
),
hdp_table_ly AS (
	SELECT
		p1.*,
		p2.GDP AS GDP_ly,
	    round( ( p1.GDP - p2.GDP ) / p2.GDP * 100, 2) as GDP_growth_percent
	FROM hdp_table p1
	LEFT JOIN hdp_table p2
		ON p1.year = p2.year + 1
)
SELECT 
	g.*,
	h.GDP_growth_percent
FROM main_table_growths g
LEFT JOIN hdp_table_ly h
	ON g.payroll_year = h.year
ORDER BY g.payroll_year asc
;