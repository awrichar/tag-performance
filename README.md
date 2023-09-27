# Database Performance for Tagging

Tool to benchmark the performance of various database technologies for a "tagging" system.
Each record can have multiple tags, so this is represented as either a many-to-many join
or an array field (depending on the database tech).

Testing methodology inspired by https://www.crunchydata.com/blog/tags-aand-postgres-arrays-a-purrfect-combination
