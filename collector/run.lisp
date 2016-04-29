;; Because lisp has a few very bad ideas about unix directories and path.

(if *load-truename*
    (progn
      (format t "Doing truename: ~A ~A" *load-truename* *load-pathname*)
      (load (merge-pathnames "load.lisp" *load-truename*)))
    (progn
      (format t "Doing default path name:~A" *default-pathname-defaults* )
      (load (merge-pathnames "collector/load.lisp" *default-pathname-defaults*))))

(in-package :ctcl)

(defun myexit ()
  (let ((code 0))
    #+allegro (excl:exit code)
    #+sbcl (sb-ext::exit)
    #+lispworks (quit)
    #+clozure (ccl::quit)
    #+cmucl (quit)

(defun run-bench () 
  (load "collector/load.lisp")
  (princ "XXX: Ensuring connections")
  (psql-ensure-connection "metis")
  ;;(princ "XXX: Dropping tables")
  (create-tables-psql "metis")
  (princ "XXX: Running Test")
  #+sbcl (cl-user::profile "CTCL")
  #+allegro
  (progn
    ;;XX(setf excl:*tenured-bytes-limit* 524288000)
    (prof:with-profiling (:type :time) (ctcl::cloudtrail-report-to-psql-async "10" "~/test-ct/"))
    (prof:show-flat-profile))
  #-allegro
  (time (ctcl::cloudtrail-report-to-psql-async "10" "~/test-ct/"))
  )


(run-bench)
