#+allegro (progn
            (load (merge-pathnames "~/quicklisp/setup.lisp" *default-pathname-defaults*))
            (ql:quickload '(:cl-date-time-parser
                            :cl-base64
                            :cl-fad
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
                            :zs3)))

(defpackage :metis
  (:use :cl :zs3
        #+allegro :prof
        )

  (:export
   #:main))
