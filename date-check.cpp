#include <iostream>
#include <sstream>

std::string
FormatDate(const std::string &old_date) { // formats date from MM-DD-YYYY to YYYY-MM-DD for comparison
	int month, day, year;                 // integers for month, day, and year
	char sep;                             // holds the dash as the separator in each date

	std::istringstream input(old_date);          // input string stream that takes the date as input
	input >> month >> sep >> day >> sep >> year; // puts everything from old_date into input, using '-' as a separator

	if (month > 12 or month < 1) { // invalid month
		return "-1";
	}
	if (month == 2 and (year % 4 != 0 or (year % 100 == 0 or year % 400 != 0) and day > 28)) { // invalid leap day
		return "-1";
	}

	std::ostringstream new_date; // out string stream to hold the correctly formatted date
	new_date.fill('0'); // if a one-number day/month is entered, pad it with a 0 at the front (i.e. 3 becomes 03)
	new_date.width(4);
	new_date << year << "-";
	new_date.width(2);
	new_date << month << "-";
	new_date.width(2);
	new_date << day;

	return new_date.str();
}

std::string
VerifyDate(const std::string &date) {
	std::string formatted = FormatDate(date); // gets the correctly formatted date for comparison
	if (formatted == "-1") {                  // catches an error in the date before truncating
		return "Error in date";
	}

	// setting minimum and maximum dates for comparison in YYYY-MM-DD format
	std::string min_postgres_date = "-4712-01-01";   // lowest Postgres date in ISO format
	std::string max_postgres_date = "5874897-12-31"; // largest Postgres date in ISO format

	if (formatted < min_postgres_date) {
		return min_postgres_date;
	}
	if (formatted > max_postgres_date) {
		return max_postgres_date;
	}

	return formatted;
}
