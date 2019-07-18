# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

context("Feather")

test_that("feather read/write round trip", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tf2 <- fs::path_abs(tempfile())
  write_feather(tib, tf2)
  expect_true(fs::file_exists(tf2))

  tf3 <- tempfile()
  stream <- FileOutputStream(tf3)
  write_feather(tib, stream)
  stream$close()
  expect_true(fs::file_exists(tf3))

  tab1 <- read_feather(tf1)
  expect_is(tab1, "data.frame")

  tab2 <- read_feather(tf2)
  expect_is(tab2, "data.frame")

  tab3 <- read_feather(tf3)
  expect_is(tab3, "data.frame")

  # reading directly from arrow::io::MemoryMappedFile
  tab4 <- read_feather(mmap_open(tf3))
  expect_is(tab4, "data.frame")

  # reading directly from arrow::io::ReadableFile
  tab5 <- read_feather(ReadableFile(tf3))
  expect_is(tab5, "data.frame")

  expect_equal(tib, tab1)
  expect_equal(tib, tab2)
  expect_equal(tib, tab3)
  expect_equal(tib, tab4)
  expect_equal(tib, tab5)

  unlink(tf1)
  unlink(tf2)
  unlink(tf3)
})

test_that("feather handles col_select = <names>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = c("x", "y"))
  expect_is(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)

  unlink(tf1)
})

test_that("feather handles col_select = <integer>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = 1:2)
  expect_is(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)
  unlink(tf1)
})

test_that("feather handles col_select = <tidyselect helper>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = everything())
  expect_identical(tib, tab1)

  tab2 <- read_feather(tf1, col_select = starts_with("x"))
  expect_identical(tab2, tib[, "x", drop = FALSE])

  tab3 <- read_feather(tf1, col_select = c(starts_with("x"), contains("y")))
  expect_identical(tab3, tib[, c("x", "y"), drop = FALSE])

  tab4 <- read_feather(tf1, col_select = -z)
  expect_identical(tab4, tib[, c("x", "y"), drop = FALSE])

  unlink(tf1)
})

test_that("feather read/write round trip", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, as_tibble = FALSE)
  expect_is(tab1, "arrow::Table")

  expect_equal(tib, as.data.frame(tab1))
  unlink(tf1)
})

test_that("feather roundtrips timestamps and timezones", {
    dates <- c(
        "2001-01-04 04:04:50",
        "2008-11-27 21:48:29",
        "1997-05-12 16:43:40",
        "2002-09-03 04:10:09",
        "2000-10-19 09:34:06",
        "2007-03-13 02:30:15",
        "2017-01-10 02:18:12",
        "2000-10-16 06:20:34",
        "1971-02-10 06:01:39",
        "1977-11-21 19:35:36"
    )
    dates <- as.POSIXct(strptime(dates, "%Y-%m-%d %H:%M:%S"), tz="CEST")
    tib1 <- tibble::tibble(x = 1:10, dates = dates)
    tf <- tempfile()
    write_feather(tib1, tf)

    expect_true(fs::file_exists(tf))
    tab1 <- read_feather(tf, as_tibble = FALSE)
    expect_is(tab1, "arrow::Table")

    tib2 <- as.data.frame(tab1)
    expect_equal(tib1, tib2)
    expect_equal(attr(tib2$dates, "tzone"), attr(tib1$dates, "tzone"))
})

test_that("feather chooses timezone Sys.timezone when not explicitly set", {
    dates <- c(
        "2001-01-04 04:04:50",
        "2008-11-27 21:48:29",
        "1997-05-12 16:43:40",
        "2002-09-03 04:10:09",
        "2000-10-19 09:34:06",
        "2007-03-13 02:30:15",
        "2017-01-10 02:18:12",
        "2000-10-16 06:20:34",
        "1971-02-10 06:01:39",
        "1977-11-21 19:35:36"
    )
    dates <- as.POSIXct(strptime(dates, "%Y-%m-%d %H:%M:%S"))
    tib1 <- tibble::tibble(x = 1:10, dates = dates)
    expect_equal(attr(tib1$dates, "tzone"), "")
    tf <- tempfile()
    write_feather(tib1, tf)

    expect_true(fs::file_exists(tf))
    tab <- read_feather(tf, as_tibble = FALSE)
    expect_is(tab, "arrow::Table")

    tib2 <- as.data.frame(tab)
    expect_equal(tib1, tib2)
    expect_equal(attr(tib2$dates, "tzone"), Sys.timezone())
})

test_that("feather serialization sets NULL timezone to GMT", {
    dates <- c(
        "2001-01-04 04:04:50",
        "2008-11-27 21:48:29",
        "1997-05-12 16:43:40",
        "2002-09-03 04:10:09",
        "2000-10-19 09:34:06",
        "2007-03-13 02:30:15",
        "2017-01-10 02:18:12",
        "2000-10-16 06:20:34",
        "1971-02-10 06:01:39",
        "1977-11-21 19:35:36"
    )
    dates <- as.POSIXct(strptime(dates, "%Y-%m-%d %H:%M:%S"), tz="CEST")
    tib1 <- tibble::tibble(x = 1:10, dates = dates)
    attr(tib1$dates, "tzone") <- NULL
    tf <- tempfile()
    write_feather(tib1, tf)

    tib2 <- read_feather(tf, as_tibble = TRUE)
    expect_equal(tib1, tib2)
    expect_equal(attr(tib2$dates, "tzone"), "GMT")
})
