#+allegro (progn
	    (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
	    (ql:quickload '(
			    :cl-date-time-parser
			    :cl-fad
			    :fare-memoization
			    :gzip-stream
			    :jonathan
			    :pcall
			    ;;:postmodern
			    :manardb
			    ;;:s-sql
			    ;;:sqlite
			    :uiop
			    )))


(defpackage :metis
  (:use :cl
	#+allegro :prof
	)

  (:export
   #:main
   #:run-bench))
