(load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
(ql:quickload '(:fare-memoization :cl-fad :gzip-stream :cl-json :s-sql :pcall :uiop :cl-store))
(load "version.lisp")

(defpackage :ctcl
  (:use :cl :rucksack :fare-memoization :cl-fad :gzip-stream :cl-json )
  #+allegro (:use :prof )
  )

