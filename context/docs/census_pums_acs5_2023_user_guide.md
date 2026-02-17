# AMERICAN COMMUNITY SURVEY 2019-2023 5-YEAR PUMS User Guide and Overview

**Pages**: 21 total pages\
**Date**: January 23, 2025\
**Author(s)**: American Community Survey Office, U.S. Census Bureau

---

## Table of Contents

I. Purpose of This Document .................................................................................................................... 4\
II. Overview of the Public Use Microdata Sample (PUMS) ....................................................................... 4\
III. PUMS Documentation .......................................................................................................................... 5\
 A. Introduction to PUMS Webinar ........................................................................................................ 5\
 B. PUMS Documentation ...................................................................................................................... 5\
 C. PUMS Handbook ............................................................................................................................... 5\
IV. Obtaining PUMS Data ........................................................................................................................... 5\
 A. ACS Website ...................................................................................................................................... 5\
 B. ACS FTP Site....................................................................................................................................... 5\
 C. Microdata Analysis Tool (MDAT) ...................................................................................................... 5\
 D. Microdata Application Programming Interface (API) ....................................................................... 6\
V. PUMS File Structure .............................................................................................................................. 6\
 A. Basic Example of Combining PUMS Person and Housing Files ......................................................... 6\
VI. PUMS Data Dictionary........................................................................................................................... 8\
 A. PUMS Data Dictionary Overview ...................................................................................................... 8\
 B. Note on PUMS Data Dictionary and Blank Values ............................................................................ 8\
 C. Explanation of Variables in Data Dictionary PDF File ........................................................................ 8\
 D. Explanation of Variables in Data Dictionary CSV File ........................................................................ 9\
 E. Organization of PUMS Variables in the Data Dictionary ................................................................. 10\
VII. PUMS Weights and Notes on calculating variances ........................................................................... 10\
 A. PUMS Weighting Variables ............................................................................................................. 10\
 B. Successive Difference Replication (SDR) Formula for Calculating Uncertainty .............................. 11\
 C. Successive Difference Replicate (SDR) Documentation .................................................................. 12\
VIII. PUMS Geographies ............................................................................................................................. 12\
 A. Geographies Available in PUMS ...................................................................................................... 12\
 B. Overview of Public Use Microdata Areas (PUMA) .......................................................................... 12\
 C. Crosswalked PUMA Codes for 2023 5-Year PUMS ......................................................................... 13\
 D. Interactive Maps Using TIGERweb .................................................................................................. 13\
 E. Static Maps for PUMAs ................................................................................................................... 14\
 F. Crosswalking PUMA Codes Using GEOCORR .................................................................................. 14\
IX. Changes to PUMS Variables for the 2023 PUMS 5-year Files ............................................................. 14\
X. Additional Notes and Useful Information ........................................................................................... 15\
 A. PUMS and Open-Source Software .................................................................................................. 15\
 B. Explanation of SAS and CSV File Names on FTP site ....................................................................... 15\
 C. Definition of the Top- and Bottom-Coded Variables ...................................................................... 15\
 D. Crosswalking Industry and Occupation Codes (INDP, NAICSP, SOCP, and OCCP) .......................... 16\
 E. Rounding Rules for Income Variables ............................................................................................. 16\
 F. Note on the PUMS Design Factors .................................................................................................. 16\
 G. Note on Income and Earnings Inflation Factor (ADJINC) ................................................................ 17\
 H. Note on Housing Dollar Inflation Factor (ADJHSG) ......................................................................... 17\
 I. Note on Standard Occupational Classification codes (SOCP) ......................................................... 18\
 J. Note on Selected Values for Industry and Occupation (INDP, NAICSP, OCCP, and SOCP) ............. 18\
 K. Codes to Identify North American Industry Classification System (NAICS) Equivalents................. 18\
 L. Additional Information on PUMS Industry and Occupation Codes ................................................ 19\
 M. Suppressed Data ............................................................................................................................. 19\
 N. Note on PUMS File Names for CSV Files ......................................................................................... 20\
 O. Additional Notes: ............................................................................................................................ 21

---

## I. PURPOSE OF THIS DOCUMENT

This document is intended to provide resources and guidance for data users on how to use the American Community Survey (ACS) Public Use Microdata Sample (PUMS) files.

---

## II. OVERVIEW OF THE PUBLIC USE MICRODATA SAMPLE (PUMS)

The Public Use Microdata Sample (PUMS) files allow data users to create estimates for user-defined characteristics. The files contain a sample of the responses to the American Community Survey (ACS). The PUMS files include variables for nearly every question on the ACS survey. Additional variables are also created from other recoded PUMS variables to provide data users with useful derived variables (such as poverty status) while protecting confidentiality and providing consistency within the PUMS files.

Please note that many estimates generated with PUMS may be different from estimates for the same characteristics published on data.census.gov. These differences are due to the fact that the PUMS microdata is a sample of the full ACS microdata and includes only about two-thirds of the records that were used to produce ACS estimates. Additional edits appropriate for PUMS were also made for confidentiality reasons.

There are two types of PUMS files, one for **Person** records and one for **Housing Unit** records. Each record in the Person file represents a single person. Individuals are organized into households, making possible the study of people within the contexts of their families and other household members. In addition, the files contain people who reside in group quarters (GQ), such as nursing homes or college dormitories. The Housing Unit files contain records for individual housing units, including vacant housing units. In addition, GQ records are also on the Housing Unit file. However, they are placeholder records that may be used solely to obtain GQ information for the variable called **FS** (“Yearly food stamp/Supplemental Nutrition Assistance Program (SNAP) recipiency”).

- PUMS files for an individual year contain data on approximately **one percent** of the United States population.
- PUMS files covering a five-year period contain data on approximately **five percent** of the United States population.

The PUMS files are much more flexible than the aggregate data provided in tables on data.census.gov, though the PUMS also tend to be more complicated to use. Working with PUMS data generally involves downloading large datasets onto a local computer and analyzing the data using statistical software such as R, SPSS, Stata, or SAS.

Since all ACS responses are strictly confidential, many variables in the PUMS files have been modified in order to protect the confidentiality of survey respondents. For instance, particularly high incomes are replaced with a top-code value and uncommon birthplace or ancestry responses are grouped into broader categories. The PUMS files also limits the geographic detail below the state level. The only substate geography provided is the **Public Use Microdata Area**, or **PUMA**.

---

## III. PUMS DOCUMENTATION

The list below provide links to documentation that are useful for PUMS users.

### A. Introduction to PUMS Webinar

Data users new to PUMS may find the “Introduction to the Public Use Microdata Sample (PUMS) File” webinar to be a useful reference. The webinar may be found at:

[https://www.census.gov/data/academy/webinars/2020/introduction-to-american-community-survey-public-use-microdata-sample-pums-files.html](https://www.census.gov/data/academy/webinars/2020/introduction-to-american-community-survey-public-use-microdata-sample-pums-files.html).

Other training, such as how to use the online Microdata Analysis Tool (MDAT), may be found at [https://www.census.gov/programs-surveys/acs/microdata/mdat.html](https://www.census.gov/programs-surveys/acs/microdata/mdat.html).

### B. PUMS Documentation

The PUMS documentation may be found by going to [https://www.census.gov/programs-surveys/acs/microdata/documentation.html](https://www.census.gov/programs-surveys/acs/microdata/documentation.html). You may find the PUMS data dictionary, PUMS Estimates for User Verification and other technical documents there.

### C. PUMS Handbook

A series of Handbooks are available that provide an overview of various aspects of the ACS. They are located at: [https://www.census.gov/programs-surveys/acs/guidance/handbooks.html](https://www.census.gov/programs-surveys/acs/guidance/handbooks.html). The PUMS Handbook is called “Understanding and Using the American Community Survey Public Use Microdata Sample Files: What Data Users Need to Know”.

---

## IV. OBTAINING PUMS DATA

PUMS data may be obtained in multiple ways.

### A. ACS Website

PUMS files can be accessed by going to [https://www.census.gov/programs-surveys/acs/microdata/access.html](https://www.census.gov/programs-surveys/acs/microdata/access.html). Links to the PUMS data via the FTP site, MDAT, and the API are available.

### B. ACS FTP Site

The PUMS Files are also available through the file transfer protocol (FTP) site at:

[https://www.census.gov/programs-surveys/acs/microdata/access.html](https://www.census.gov/programs-surveys/acs/microdata/access.html).

Data users may find a list of state names and abbreviations useful. The information may be obtained here: [https://www.census.gov/library/reference/code-lists/ansi/ansi-codes-for-states.html](https://www.census.gov/library/reference/code-lists/ansi/ansi-codes-for-states.html). Click on “FIPS Codes for the States and District of Columbia” to obtain the state abbreviations.

### C. Microdata Analysis Tool (MDAT)

The Microdata Analysis Tool (MDAT) may be found at: [https://data.census.gov/mdat/](https://data.census.gov/mdat/).

The tool may be used to create estimates online without the use of statistical software. Note that the tool may change in the future. It is still under development and in beta form. As a note, the tool cannot currently inflation adjust PUMS variables. This capability will be added as a future enhancement.

Data users may find the webinar “Taking the Guesswork Out of MDAT” useful in learning how to use MDAT. It may be found here:

[https://www.census.gov/data/academy/webinars/2024/taking-the-guesswork-out-of-mdat.html](https://www.census.gov/data/academy/webinars/2024/taking-the-guesswork-out-of-mdat.html)

### D. Microdata Application Programming Interface (API)

PUMS data may also be obtained using the Census Microdata API. It may be found here: [https://www.census.gov/data/developers/data-sets/census-microdata-api.html](https://www.census.gov/data/developers/data-sets/census-microdata-api.html).

Guidance on how to use the Microdata API may be found here: [https://www.census.gov/data/developers/guidance.html](https://www.census.gov/data/developers/guidance.html).

---

## V. PUMS FILE STRUCTURE

The ACS questionnaire contains household items that are the same for all members of the household (such as the number of rooms in the home) and person items that are unique for each household member (such as age, sex, and race). The ACS PUMS files are made available in this same structure. Researchers who are analyzing only household-level items may use the housing unit files, whereas those using only person-level variables may use the person files.

The person files also contain records for persons in group quarter facilities (such as nursing homes or college dorms). The housing unit files contain placeholder records for group quarters. The majority of the variables for housing unit records for group quarters are blank. The weights and replicate weights are zero. The group quarter place holder records exist so that data users may obtain values for the variable **FS** (Yearly food stamp/Supplemental Nutrition Assistance Program recipiency).

PUMS files containing data for the entire United States are separated into several files due to their size. For 5-year PUMS data, there are four files (“a” through “d”). More information is provided at the end of this document.

### A. Basic Example of Combining PUMS Person and Housing Files

Below are instructions for concatenating the two 1-year “a” and “b” PUMS files to create a single national file. The code uses SAS programming code (copyright © 2024 SAS Institute Inc.; SAS and all other SAS Institute Inc. product or service names are registered trademarks or trademarks of SAS Institute Inc., Cary, NC, USA). See section X (below) for a link to open-source software (R and Python) that may be used to work with PUMS files.

**Concatenate the person-level files using the set statement:**

```sas
data population;
set psam_pusa psam_pusb;
run;
```

**Concatenate the household-level files using the set statement:**

```sas
data housing;
set psam_husa psam_husb;
run;
```

As mentioned above, if the data user is using 5-year PUMS files they will need to concatenate four files (`psam_husa` through `psam_husd`) together.

Some data users will need to use household and person items together. For instance, in order to analyze how the number of rooms in a home varies by a person’s age, merge the household and person files together using the serial number variable (**SERIALNO**).

**First make sure the files are sorted by SERIALNO.**

```sas
proc sort data=population;
by serialno;
run;
proc sort data=housing;
by serialno;
run;
```

**Then merge the two files together using SERIALNO as a merge key.** Note that in SAS, the ‘in=’ option will allow you to identify records from a specific file. The line ‘if pop’ retains only records from the population file.

```sas
data combined;
merge population (in=pop) housing;
by serialno;
if pop;
run;
```

You do not need to merge the files unless the estimates you wish to create require a merge. Note that there are many estimates that may be tabulated from the person file and from the household file without any merging. The suggested merge will create a person level file, so that the estimate of persons may be tallied within categories from the household file and the person weights should be used for such tallies.

Note also that the housing unit record files contain vacant housing units. There are no population records for these housing units.

---

## VI. PUMS DATA DICTIONARY

### A. PUMS Data Dictionary Overview

The PUMS Data Dictionary provides the values for each PUMS variable, as well as labels for each value. For example, on the PUMS files, if the PUMS variable “REGION” has a value of “3”, a data user may use the Data Dictionary to see that “3” means “South”.

The PUMS Data Dictionary is published in three different formats. There is a text version, a pdf version, and a comma-separated values (CSV) version. The information in each version is equivalent to one another.

### B. Note on PUMS Data Dictionary and Blank Values

Records in PUMS that are not within the universe for a variable are given blank values. For example, for the PUMS variable Educational Attainment (SCHL), the universe is people age 3 or older. Person records with an age less than 3 have a blank value for SCHL.

The PUMS Data Dictionary represents blank values as a series of **b**’s. For example, Educational Attainment has a length of 2. In the PUMS Data Dictionary, blank values for SCHL are displayed as “bb”. The PUMS files do not use b’s to denote blanks. Instead, they are either a numeric blank (for numeric variables) or a character blank value (for character variables).

However, blanks are handled differently in MDAT and the Census API, where an actual blank value is not allowed. There, blanks are generally represented as one less than the lowest legal value. For example, if the lowest legal value of a variable was 0, then blanks would be represented by -1. For some variables, a blank value is represented by ‘N’ instead. For information about blank values of a specific variable in MDAT or the API, see the 2023 5-year PUMS API data dictionary at [https://api.census.gov/data/2023/acs/acs5/pums/variables.html](https://api.census.gov/data/2023/acs/acs5/pums/variables.html) and click on the variable name. MDAT users can also click Details on the Select Variables page.

### C. Explanation of Variables in Data Dictionary PDF File

Below is an example of the PUMS variable for Record Type (RT). The first line shows the PUMS variable name (RT), followed by “Character” to indicate it is a character variable and the number 1 to indicate that the length of the variable is one. The next line provides the descriptive title for the variable (“Record Type”). The remaining two lines provide the PUMS values for the variable and their appropriate labels. For example, when RT = “H”, this stands for “Housing Record or Group Quarters Unit”.

**Example of PUMS Data Dictionary (PDF Version)**

| Variable | Type      | Length |
| -------- | --------- | ------ |
| RT       | Character | 1      |

**Record Type**

| Value | Label                                 |
| ----- | ------------------------------------- |
| H     | Housing Record or Group Quarters Unit |
| P     | Person Record                         |

### D. Explanation of Variables in Data Dictionary CSV File

Below is an example of how the Data Dictionary appears in the CSV version.

**Example of Data Dictionary from CSV file**

```
NAME,RT,C,1,"Record Type"
VAL,RT,C,1,"H","H","Housing Record or Group Quarters Unit"
VAL,RT,C,1,"P","P","Person Record"
```

The position of the variables (from left to right in the file) is provided in the table below. The file itself contains no variable names. That is, the first line of the file represents records for the data. Note that lines starting with “NAME” contain five variables and is equivalent to the first two lines in the PDF example (above). Lines starting with “VAL” have seven variables and include the variable starting and ending values and labels.

**PUMS Variables in Data Dictionary CSV File**

| Position | Variable                                 | Description                                                                                     |
| -------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------- |
| 1        | Identifying Flag (ID flag)               | “NAME” for information about the variable; “VAL” for values of the variable                     |
| 2        | PUMS Variable Name                       | PUMS variable name (e.g. RT, SERIALNO, AGEP, etc.)                                              |
| 3        | Variable Type                            | “C” for Character variable; “N” for Numeric variable (most variables are character)             |
| 4        | Length                                   | Length of PUMS variable                                                                         |
| 5        | Descriptive Title / Starting Legal Value | Descriptive Title (ID Flag = “NAME”); Starting value for variable value range (ID Flag = “VAL”) |
| 6        | Ending Legal Value                       | Ending value for value variable range (ID Flag = “VAL”)                                         |
| 7        | Description                              | Descriptive name (ID Flag = “VAL”)                                                              |

The text and CSV versions may be read in to statistical programs to create formats for the PUMS variables. Note that the CSV version was first published for 2017 PUMS data. For 2016 and earlier data, only the text version is available.

### E. Organization of PUMS Variables in the Data Dictionary

The PUMS variables are placed into groups within the Data Dictionary. The Housing variables come first, followed by the Person variables. The variables are further divided into categories which are listed below. These sections and subsections are provided in the pdf and text versions of the Data Dictionary, but not the CSV version. The CSV version is intended to be machine-readable. Therefore, only variables and their values are present in that file.

**Major Variable Organizational Categories**

| Data Dictionary Section  | Description                                                                      |
| ------------------------ | -------------------------------------------------------------------------------- |
| **HOUSING RECORD**       |                                                                                  |
| BASIC VARIABLES          | Basic variables, such as geographic variables and inflation adjustment variables |
| HOUSING UNIT VARIABLES   | Housing variables pertaining to the Housing Unit                                 |
| HOUSEHOLD VARIABLES      | Housing variables pertaining to the Household                                    |
| ALLOCATION FLAGS         | Housing allocation flag variables                                                |
| REPLICATE WEIGHTS        | Housing replicate weight variables used for variance calculation                 |
| **PERSON RECORD**        |                                                                                  |
| BASIC VARIABLES          | Basic variables, such as geographic variables and inflation adjustment variables |
| PERSON VARIABLES         | Person Variables                                                                 |
| RECODED PERSON VARIABLES | PUMS Person Variables created from other Variables                               |
| ALLOCATION FLAGS         | Person allocation flag variables                                                 |
| REPLICATE WEIGHTS        | Person replicate weight variables used for variance calculation                  |

---

## VII. PUMS WEIGHTS AND NOTES ON CALCULATING VARIANCES

### A. PUMS Weighting Variables

The ACS PUMS is a weighted sample. Weighting variables must be used in order to calculate estimates which represent the actual population. Weighting variables are also needed to generate accurate measures of uncertainty, such as the standard error or margin of error. The PUMS files include both population weights (in the Person files) and household weights (located in the Housing files). Population weights should be used to generate statistics about individuals, and household weights should be used to generate statistics about housing units or households. The weighting variables are described briefly below.

- **PWGTP**: Person weight for generating statistics on individuals (such as age).
- **PWGTP1–PWGTP80**: Replicate Person weighting variables, used for generating the standard error and margin of error for person characteristics.
- **WGTP**: Housing unit weight for generating statistics on housing units and households (such as household income).
- **WGTP1–WGTP80**: Replicate Housing Unit weighting variables, used for generating the standard error and margin of error for housing unit and household characteristics.

The PUMS Weighting variables (PWGTP and WGTP) may both be used to generate PUMS estimates. They are also used in the generalized variance formulas (GVF) method for calculating standard errors using the design factors. Replicate weights may only be used to calculate standard errors and margins of error using the successive difference replication (SDR) method. The SDR method may also be referred to as direct standard errors.

### B. Successive Difference Replication (SDR) Formula for Calculating Uncertainty

The ACS uses the SDR methodology to calculate margins of error for published data products. The SDR method is discussed in the Accuracy of the PUMS document, located at: [https://www.census.gov/programs-surveys/acs/microdata/documentation.html](https://www.census.gov/programs-surveys/acs/microdata/documentation.html).

Note that there is also a generalized variance formula (GVF) method for calculating standard errors and margins of error. The GVF method uses design factors. Worked examples are provided in the Accuracy of the PUMS document.

As previously mentioned, each housing unit and person record contains 80 replicate weights. To use the replicate weights to calculate an estimate of the SDR standard error:

1. Calculate the PUMS estimate using the PUMS weight (either PWGTP or WGTP).
2. Calculate 80 replicate estimates, using each of the 80 replicate weights. For example, for the first replicate estimate, use the first replicate weight (e.g. PWGTP1 instead of PWGTP, or WGTP1 instead of WGTP).
3. Calculate the variance by first taking the difference between each replicate estimate and the PUMS estimate. Square each of these differences, and then sum the 80 squared differences. Multiply this sum by the quantity $4/80$. The 4 is required to remove bias, while 80 is present due to the 80 replicate estimates.

**Equation (SDR variance):**

$$
\text{Variance} = \frac{4}{80} \sum_{r=1}^{80} (x_r - x)^2
$$

In the equation, $x_r$ is the $r$-th replicate estimate, and $x$ is the full PUMS weighted estimate.

To obtain the standard error (SE), take the square root of the variance. To obtain a 90% confidence level margin of error, multiply the SE by 1.645.

### C. Successive Difference Replicate (SDR) Documentation

The webinar called “Calculating Margins of Error the ACS Way” provides an overview on how to calculate variance, standard errors, and margins of error using the SDR formula. It provides a worked example using PUMS data. It is located at:

[https://www.census.gov/data/academy/webinars/2020/calculating-margins-of-error-acs.html](https://www.census.gov/data/academy/webinars/2020/calculating-margins-of-error-acs.html).

Another reference for how to use the SDR formula is provided in the Variance Replicate Estimate (VRE) Tables Documentation, located at: [https://www.census.gov/programs-surveys/acs/data/variance-tables.html](https://www.census.gov/programs-surveys/acs/data/variance-tables.html). Although the VRE documentation pertains to ACS data, the concepts provided in the documentation may be adopted for use with PUMS data. This document presents worked examples using the ACS VRE tables.

The technical explanation of the creation of the ACS replicate weights may be found in Chapter 12 of the Design and Methodology document located at: [https://www.census.gov/programs-surveys/acs/methodology/design-and-methodology.html](https://www.census.gov/programs-surveys/acs/methodology/design-and-methodology.html).

---

## VIII. PUMS GEOGRAPHIES

The following sections provide an overview on geographies available in the PUMS files.

### A. Geographies Available in PUMS

In order to protect confidentiality, a limited number of geographic summary levels are available on the PUMS files. They include **region**, **division**, **state** and **Public Use Microdata Area (PUMA)**.

Division is a subdivision of the region summary level. An example of region is “Northeast”, while a division is “New England”. The PUMS variable for regional division is called “DIVISION”.

In addition to the 50 states, there are also two state equivalents: the District of Columbia and Puerto Rico. Records for the District of Columbia are included in the PUMS files for the nation. Puerto Rico data is only available as a state-level file.

### B. Overview of Public Use Microdata Areas (PUMA)

While PUMS files contain records from across the nation, towns and counties (and other low-level geography) are not identified by any variables in the PUMS datasets. The most detailed unit of geography contained in the PUMS files is **PUMA**.

PUMAs are special non-overlapping areas that partition each state into contiguous geographic units containing roughly 100,000 people at the time of their creation. They are created after each Decennial Census. The current PUMS files use the PUMA definitions created after Census 2020.

PUMAs are identified by a 5-digit code. Note that you must use the state variable (**ST**) along with the PUMA code to uniquely identify an individual PUMA. PUMA codes are unique within a state, but not between states. For example, the PUMA code “00100” is used in both Connecticut and North Dakota.

### C. Crosswalked PUMA Codes for 2023 5-Year PUMS

The current PUMA boundaries are based on Census 2020 definitions, while records from 2021 and earlier originally used boundaries based on Census 2010 definitions.

For the 2023 5-year PUMS files, the older 2010-based PUMA codes for 2019, 2020, and 2021 records have been crosswalked to the 2020-based PUMA codes using a PUMA-to-PUMA relationship file. So, the 2023 (and subsequent) 5-year PUMS files will have a single 2020-based PUMA code (PUMA), as opposed to the 2022 5-year PUMS files which had two separate, non-crosswalked PUMA codes (PUMA10 and PUMA20). Similarly, the Place of Work PUMA (**POWPUMA**) and Migration PUMA (**MIGPUMA**) have been crosswalked as well, and have 2020-based codes for the 2023 5-year PUMS files and future years.

### D. Interactive Maps Using TIGERweb

The Census Bureau provides an interactive mapping application, called TIGERweb. Data users can view PUMA boundaries from 2020. TIGERweb is available at: [https://tigerweb.geo.census.gov/tigerwebmain/tigerweb_main.html](https://tigerweb.geo.census.gov/tigerwebmain/tigerweb_main.html).

To access the maps:

1. Click on “TIGERweb Applications” on the upper left.
2. Click “TIGERweb Decennial” on the left column. This will take you to a new page.
3. On the upper left, you should see choices: “Layers”, “Legend”, or “Task Results”. “Layers” should by default be selected. If not, then select it.
4. Under “Select Vintage:” choose “Census 2020”.
5. Select “PUMAs, UGAs, and ZCTAs” on the left.
6. Click on the map to zoom in or move the zoom scale bar to zoom in closer to the map.
7. Expand the “PUMAs, UGAs, and ZCTAs” box to see the choices. Make sure that the box with “Public Use Microdata Areas” is selected. Unselect other options.
8. Alternately, you may check other boxes to add or remove geographic summary levels and other features, such as “Hydrography”. Use the plus sign (“+”) to see more detail for a particular selection.
9. In the upper right, click on the icon of the gears next to the search bar (that says “Street, City, State, Zip”). This will allow you to compare 2010 to 2020 PUMA boundaries.
10. In the upper right, click on the icon with a question mark. A pop-up will appear with a link to the Tigerweb User Guide. The User Guide will provide instructions on how to compare the two PUMA vintages.

### E. Static Maps for PUMAs

Data users may be interested in static maps of PUMA boundaries. These may be found at: [https://www.census.gov/programs-surveys/geography.html](https://www.census.gov/programs-surveys/geography.html).

To access the static maps:

1. Click on “Geographies” on the left hand side.
2. Click on “Geography Reference Maps”.
3. Click on the year “2020”.
4. Scroll down and click on “2020 Census Public Use Microdata Area (PUMA) Reference Maps”.
5. Choose the state you are interested in from the dropdown menu.
6. You will be redirected to the relevant Census FTP site. Within the site, you will see a file that will begin with the label “Catalog_PUMAmaps” followed by the state code. For example, if you had selected Idaho (state code “16”) in the previous step, this file is called `Catalog_PUMAmaps_st16.pdf`.
7. This catalog will provide the list of state and PUMA codes to select the relevant map. For example, for Idaho (state code “16”) and PUMA code “00702”, it has the description “Ada County (Northeast)--Boise (North & West) & Garden City Cities”. To see the maps for that PUMA, click on the file called `DC20PUMA_1600702.pdf`.

### F. Crosswalking PUMA Codes Using GEOCORR

GEOCORR, which stands for “Geographic Correspondence Engine”, was developed by the Missouri Census Data Center (MCDC), which is part of the Census Bureau’s State Data Center program ([https://www.census.gov/about/partners/sdc.html](https://www.census.gov/about/partners/sdc.html)).

Among other things, the software allows data users to calculate the proportion of a PUMA code from Census 2010 that lies within the new PUMA codes from Census 2020. It also provides the data user with an allocation factor so that they may crosswalk old PUMA codes to new PUMA codes. More information on GEOCORR may be found at: [http://mcdc.missouri.edu/applications/geocorr.html](http://mcdc.missouri.edu/applications/geocorr.html).

There is a Data Gem presentation on how to use GEOCORR to crosswalk geographies. It may be found here: [https://www.census.gov/data/academy/data-gems/2021/how-to-use-the-geocorr-to-identify-the-geographies-that-make-up-your-area.html](https://www.census.gov/data/academy/data-gems/2021/how-to-use-the-geocorr-to-identify-the-geographies-that-make-up-your-area.html).

---

## IX. CHANGES TO PUMS VARIABLES FOR THE 2023 PUMS 5-YEAR FILES

Changes to variables from last year are noted in a document called “2023 ACS 5-year PUMS Variable Changes and Explanations”. It may be found at: [https://www.census.gov/programs-surveys/acs/microdata/documentation.html](https://www.census.gov/programs-surveys/acs/microdata/documentation.html).

---

## X. ADDITIONAL NOTES AND USEFUL INFORMATION

### A. PUMS and Open-Source Software

Data users may be interested in working with PUMS using open-source software, such as R and Python. Links to some useful packages may be found at: [https://www.census.gov/programs-surveys/acs/guidance/statistical-software.html](https://www.census.gov/programs-surveys/acs/guidance/statistical-software.html).

### B. Explanation of SAS and CSV File Names on FTP site

There are two types of zipped PUMS files on the PUMS FTP site (located at: [https://www2.census.gov/programs-surveys/acs/data/pums/](https://www2.census.gov/programs-surveys/acs/data/pums/)). One version is for the comma separated version (CSV) files of the PUMS data. The other is for the SAS version of the files.

- Beginning with data year **2021**, the SAS files will start with **“sas_”**. For example, the PUMS person file for Alabama will be named “sas_pal.zip”. The “p” is for Person, and “al” is the state abbreviation for Alabama.
- The SAS files from **2010 through 2020** begin with **“unix_”**. For example, the Person file for Alabama would be called “unix_pal.zip”.
- For **2009 and earlier**, there are two SAS files. One begins with “pc_” and the other begins with “unix_”. In the past, pc SAS files and Unix SAS files were not compatible. Therefore, two files were published.

### C. Definition of the Top- and Bottom-Coded Variables

To protect confidentiality, selected variables are **top-coded**. A process identifies records which meet or exceed the specific top-code threshold value. These records are replaced with the mean value. Note that the distribution from the full ACS sample is used to calculate the mean value. There are separate top-code values for each state. The threshold value is calculated by identifying the top half percent or top three percent value using the distribution of the full microdata.

The following PUMS variables use the **half percent** threshold:

AGEP, BDSP, ELEP, GASP, INSP, JWMNP, MRGP, RMSP, RNTP, SSP, TAXAMT, VALP, WAGP, WATP

The following PUMS variables use the **three percent** threshold:

CONP, FULP, INTP, MHP, OIP, PAP, RETP, SEMP, SMP, SSIP

The “PUMS Top Coded & Bottom Coded Values” document contains the threshold value and mean value for select variables by state. The top-code threshold variables end in “TPCT”. Beginning in 2017, the top-code value variables begin with “T_” (e.g. “T_ELE”). For 2009 through 2016, the top-code value variables had the same name as the PUMS variable that was top-coded (e.g. “ELEP”). Prior to 2009, the P at the end of the variable was not used (e.g. “ELE”).

In addition, two variables are **bottom-coded**. Bottom-coding is similar to top-coding. It identifies records which are at or below the bottom-coded threshold value and replaces them with the mean value of all records less than or equal to the bottom-coded threshold. The threshold value below which variables are bottom-coded end in “BPCT”. Similarly to the top-codes, beginning in 2017 the variables for the bottom-coded values begin with “B_”. For 2016 and earlier, the bottom-coded variables are named “BINT” and “BSEM”.

Please note that the variable names for the top- and bottom-code threshold and mean variables are **not** on the PUMS microdata files. For a given state, if a record in the PUMS file has a value at or above the top-code threshold, the value is replaced with the appropriate top-code value.

### D. Crosswalking Industry and Occupation Codes (INDP, NAICSP, SOCP, and OCCP)

The Census Industry codes were updated in 2023 due to revision of the 2022 North American Industry Classification System (NAICS). The Census Occupation codes were last updated in 2018. Data users who wish to crosswalk industry codes from 2022 and earlier or occupation codes from 2017 and earlier may do so using the ACS Code Lists and Crosswalks, which are found here: [https://www.census.gov/topics/employment/industry-occupation/guidance.html](https://www.census.gov/topics/employment/industry-occupation/guidance.html).

Information on how the ACS codes are collapsed into PUMS codes may be found in the PUMS Code Lists documentation located at [https://www.census.gov/programs-surveys/acs/microdata/documentation.html](https://www.census.gov/programs-surveys/acs/microdata/documentation.html).

### E. Rounding Rules for Income Variables

PUMS income variables are subject to rounding rules, displayed in the table below.

| Range                     | Rounding Rule             | Unrounded Example | Rounded Example |
| ------------------------- | ------------------------- | ----------------- | --------------- |
| 0                         | 0 (No Rounding)           | 0                 | 0               |
| $0 < X \leq 7$            | 4                         | 6                 | 4               |
| $7 < X \leq 999$          | Nearest ten               | 12                | 10              |
| $999 < X \leq 49{,}999$   | Nearest hundred           | 5,234             | 5,200           |
| $X > 49{,}999$            | Nearest thousand          | 54,123            | 54,000          |
| $-7 \leq X < 0$           | -4                        | -6                | -4              |
| $-999 \leq X < -7$        | Nearest negative ten      | -12               | -10             |
| $-49{,}999 \leq X < -999$ | Nearest negative hundred  | -5,234            | -5,200          |
| $X \leq -49{,}999$        | Nearest negative thousand | -54,123           | -54,000         |

### F. Note on the PUMS Design Factors

The PUMS design factors used to calculate standard errors in the generalized variance formulas (GVF) method are periodically updated as the need arises. For example, if new variables are included on the PUMS file, additional design factors may be added. The design factors are not updated every year. They were not updated for the 2023 PUMS 5-year files. However, two variable names were changed (STATE to STATENAME, and ST to STATE), to meet new internal systems requirements.

For 2016 and earlier, the design factors were published in the PUMS Accuracy document. The values were given in tables at the end of the document. Beginning in 2017, the design factors were published as a csv file.

A description of the design factor variables in the csv file is provided in the table below.

**PUMS Design Factor Variables in CSV File**

| Variable       | Description                                            |
| -------------- | ------------------------------------------------------ |
| YEAR           | 4-digit year                                           |
| PERIOD         | Time period (1-year or 5-year)                         |
| STATENAME      | State Name                                             |
| STATE          | State FIPS Code                                        |
| CHARTYP        | Characteristic Type (either “POPULATION” or “HOUSING”) |
| CHARACTERISTIC | Description of PUMS Design Factor Characteristic Group |
| DESIGN_FACTOR  | Design Factor                                          |

### G. Note on Income and Earnings Inflation Factor (ADJINC)

Divide ADJINC by 1,000,000 to obtain the inflation adjustment factor and multiply it to the PUMS variable value to adjust it to **2023 dollars**. Variables requiring ADJINC on the Housing Unit file are **FINCP** and **HINCP**. Variables requiring ADJINC on the Person files are: INTP, OIP, PAP, PERNP, PINCP, RETP, SEMP, SSIP, SSP, and WAGP.

### H. Note on Housing Dollar Inflation Factor (ADJHSG)

Divide ADJHSG by 1,000,000 to obtain the inflation adjustment factor and multiply it to the PUMS variable value to adjust it to **2023 dollars**. Variables requiring ADJHSG on the Housing Unit files are: CONP, ELEP, FULP, GASP, GRNTP, INSP, MHP, MRGP, SMOCP, RNTP, SMP, TAXAMT, VALP, and WATP.

Housing value (VALP) is now required to be inflation adjusted, as of 2022. For more information, see the user note: [https://www.census.gov/programs-surveys/acs/technical-documentation/user-notes/2023-07.html](https://www.census.gov/programs-surveys/acs/technical-documentation/user-notes/2023-07.html).

For PUMS 1-year data, ADJHSG has a value of 1,000,000 (i.e., a housing dollar inflation factor of 1). Consult the PUMS Data Dictionary for the values for the 5-year files.

Note that TAXAMT is inflation adjusted. In the past TAXP was not due to it being a categorical variable. Note that ADJHSG does not apply to AGS because it is a categorical variable. If data users convert the categories in AGS to a numeric value (for example, using the midpoint of the range of each category), then they may apply the inflation factor.

### I. Note on Standard Occupational Classification codes (SOCP)

In cases where the Standard Occupational Classification (SOCP) codes ends in X(s) or Y(s), two or more SOC occupation codes were aggregated to correspond to a specific PUMS SOCP code. In these cases, the PUMS occupation description is used for the SOC occupation title.

Additional information on Occupation groupings within major categories may be found at: [https://www.census.gov/topics/employment/industry-occupation/guidance/indexes.html](https://www.census.gov/topics/employment/industry-occupation/guidance/indexes.html).

### J. Note on Selected Values for Industry and Occupation (INDP, NAICSP, OCCP, and SOCP)

Some codes are pseudo-codes developed by the Census Bureau and are not official NAICS, industry, or occupation codes.

**Pseudo-Codes Values for Select Variables**

| Variable | Value  | Description                                                                        |
| -------- | ------ | ---------------------------------------------------------------------------------- |
| SOCP     | 999920 | Unemployed, With No Work Experience In The Last 5 Years Or Earlier Or Never Worked |
| NAICSP   | 999920 | Unemployed, With No Work Experience In The Last 5 Years Or Earlier Or Never Worked |
| SOCP     | 559830 | MIL-Military, Rank Not Specified                                                   |
| OCCP     | 9920   | Unemployed, With No Work Experience In The Last 5 Years Or Earlier Or Never Worked |
| INDP     | 9920   | Unemployed, With No Work Experience In The Last 5 Years Or Earlier Or Never Worked |

### K. Codes to Identify North American Industry Classification System (NAICS) Equivalents

Data users may notice that some values of the PUMS variable NAICSP contain letters in addition to numbers. The table below provides an explanation of these letters.

**Description of Special Letters in NAICSP Variable**

| Code | Description                                                                   |
| ---- | ----------------------------------------------------------------------------- |
| M    | Multiple NAICS codes                                                          |
| P    | Part of a NAICS code - NAICS code split between two or more Census codes      |
| S    | Not specified Industry in NAICS sector - Specific to Census codes only        |
| Z    | Exception to NAICS code - Part of NAICS industry but has a unique Census code |

Additional information on NAICS may be found at: [https://www.census.gov/topics/employment/industry-occupation/guidance/indexes.html](https://www.census.gov/topics/employment/industry-occupation/guidance/indexes.html). Note that NAICS is pronounced “nakes”.

### L. Additional Information on PUMS Industry and Occupation Codes

Data users may wish to consult the Code Lists at [https://www.census.gov/programs-surveys/acs/microdata/documentation.html](https://www.census.gov/programs-surveys/acs/microdata/documentation.html) for more information on how industry and occupation codes are mapped to PUMS industry and occupation codes.

For additional information on NAICS and SOC groupings within major categories see the Industry and Occupation page, located at: [https://www.census.gov/topics/employment/industry-occupation.html](https://www.census.gov/topics/employment/industry-occupation.html).

### M. Suppressed Data

Data errors in the ACS are sometimes too complex, or are found too late, to fix on the PUMS files. In those situations, specific cases are suppressed using a unique code. The following variables have some data suppressed on the 2023 5-year PUMS files.

- **Journey to work variables** in Nueces County, Texas, for PUMA codes 06601, 06603, and 06604, for 2023. Users should be especially careful if they are calculating mean travel time to work for an area that includes these PUMAs. For more information, see the errata: [https://www.census.gov/programs-surveys/acs/technical-documentation/errata/146.html](https://www.census.gov/programs-surveys/acs/technical-documentation/errata/146.html).

| Variables Suppressed | Variable Description    | Suppression Code |
| -------------------- | ----------------------- | ---------------- |
| JWMNP                | Travel time to work     | 888              |
| JWAP                 | Time of arrival to work | 888              |

- **Internet variables** in Lackawanna County, Pennsylvania, for PUMA code 00701, for 2023. For more information see the errata: [https://www.census.gov/programs-surveys/acs/technical-documentation/errata/145.html](https://www.census.gov/programs-surveys/acs/technical-documentation/errata/145.html).

| Variables Suppressed | Variable Description                    | Suppression Code |
| -------------------- | --------------------------------------- | ---------------- |
| BROADBND             | Cellular data plan for a smartphone     | 8                |
| HISPEED              | Broadband (high speed) Internet service | 8                |
| OTSHVCEX             | Other Internet service                  | 8                |
| SATELLITE            | Satellite Internet service              | 8                |
| DIALUP               | Dial-up service                         | 8                |

- **Migration PUMA and migration state codes** for many records in Connecticut and scattered other records across the country, for 2022. For more information see the errata: [https://www.census.gov/programs-surveys/acs/technical-documentation/errata/143.html](https://www.census.gov/programs-surveys/acs/technical-documentation/errata/143.html).

| Variables Suppressed | Variable Description | Suppression Code |
| -------------------- | -------------------- | ---------------- |
| MIGPUMA              | Migration PUMA       | 88888            |
| MIGSP                | Migration state      | 888              |

- **Telephone service variable** in Flagler County and Volusia County, Florida, for PUMA codes 03500 and 12702, and in St. Joseph County, Indiana, for PUMA codes 00401 and 00402, for 2019. For more information see the errata: [https://www.census.gov/programs-surveys/acs/technical-documentation/errata/118.html](https://www.census.gov/programs-surveys/acs/technical-documentation/errata/118.html).

| Variables Suppressed | Variable Description | Suppression Code |
| -------------------- | -------------------- | ---------------- |
| TEL                  | Telephone service    | 8                |

### N. Note on PUMS File Names for CSV Files

Data users may download PUMS data in either a CSV file or as a SAS file. Beginning with 2017 data, the CSV file will have the same name as the SAS file. For Person-level files, the name is **“PSAM_P&lt;ST&gt;”** and for Housing-level files, the name is **“PSAM_H&lt;ST&gt;”**. Here, &lt;ST&gt; is the State FIPS code.

State names, abbreviations and FIPS codes may be found here: [https://www.census.gov/library/reference/code-lists/ansi.html](https://www.census.gov/library/reference/code-lists/ansi.html). Choose the “State and State Equivalents” link. FIPS Codes are 2-digit codes. For example, for Connecticut, &lt;ST&gt; is “09”.

For the 5-year data, there are four files, an “A”, “B”, “C”, and “D” file. For Person-level data the names are “PSAM_PUSA”, “PSAM_PUSB”, “PSAM_PUSC”, and “PSAM_PUSD”. The Housing-level files are “PSAM_HUSA”, “PSAM_HUSB”, “PSAM_HUSC”, and “PSAM_HUSD”.

**States in PUMS 5-year National Files**

| File | First State  | First State FIPS Code | Last State  | Last State FIPS Code |
| ---- | ------------ | --------------------- | ----------- | -------------------- |
| A    | Alabama      | 01                    | Hawaii      | 15                   |
| B    | Idaho        | 16                    | Mississippi | 28                   |
| C    | Missouri     | 29                    | Oregon      | 41                   |
| D    | Pennsylvania | 42                    | Wyoming     | 56                   |

Puerto Rico data is not included in the national files. It is published as a state equivalent and has a State FIPS code of “72”.

### O. Additional Notes:

The Census Bureau occasionally provides corrections or updates to PUMS files. Data users may sign up for notifications and updates via the Census Bureau’s E-mail Updates system at: [https://service.govdelivery.com/accounts/USCENSUS/subscriber/new?category_id=USCENSUS_C12](https://service.govdelivery.com/accounts/USCENSUS/subscriber/new?category_id=USCENSUS_C12).

In addition, PUMS errata notes may be found here: [https://www.census.gov/programs-surveys/acs/technical-documentation/errata.html](https://www.census.gov/programs-surveys/acs/technical-documentation/errata.html).

User notes are located here: [https://www.census.gov/programs-surveys/acs/technical-documentation/user-notes.html](https://www.census.gov/programs-surveys/acs/technical-documentation/user-notes.html).

Data users may also email **acso.users.support@census.gov** with any PUMS-related questions.
