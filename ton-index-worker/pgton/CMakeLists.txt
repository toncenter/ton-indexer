cmake_minimum_required(VERSION 3.10)
project(pgton)

find_package(PostgreSQL REQUIRED)
add_postgresql_extension(pgton
  VERSION 0.1
  SOURCES pgton.c
  SCRIPTS pgton--0.1.sql
  REGRESS basic)

enable_testing()
