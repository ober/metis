#+allegro (progn
	    (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
	    (ql:quickload '(:cl-date-time-parser
			    :cl-fad
			    :fare-memoization
			    :gzip-stream
			    :jonathan
			    :manardb
			    :pcall
			    :salza2
			    :cl-base64
                :thnappy
			    :trivial-garbage
			    :uiop
			    :usocket
			    :zs3)))

(defpackage :metis
  (:use :cl :zs3
	#+allegro :prof)

  (:export
   #:main))
