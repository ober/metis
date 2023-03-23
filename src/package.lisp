#+allegro (progn
            (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
            (ql:quickload '(:cl-date-time-parser
                            :cl-base64
                            :cl-fad
                            :cl-ssdb
                            :fare-memoization
                            :for
                            :gzip-stream
                            :manardb
                            :pcall
                            :salza2
                            :shasht
                            :trivial-garbage
                            :uiop
                            :usocket
                            )))

(defpackage :metis
  (:use :cl
        #+allegro :prof
        )

  (:export
   #:main))
