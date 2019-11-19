# module_matching
Code sample November 2019

The company introduces a new product: a pharmacist at the time of sale gets information about the drug,
possible replacements and additional sales. My job was to collect that data and to create a system
that can take a list of goods and return it filled with data.
The most complicated and time-consuming part of this problem is to match the pharmacy's nomenclature
to the bonus SKUs because the number of variants of writing each title can reach several thousand.
The problem successfully solved.
Local soft connects to a server to get that info. This code is a part of the server's backend.