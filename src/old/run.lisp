;; Because lisp has a few very bad ideas about unix directories and path.

(if *load-truename*
    (progn
      (format t "Doing truename: ~A ~A" *load-truename* *load-pathname*)
      (load (merge-pathnames "load.lisp" *load-truename*)))
    (progn
      (format t "Doing default path name:~A" *default-pathname-defaults* )
      (load (merge-pathnames "collector/load.lisp" *default-pathname-defaults*))))

(in-package :ctcl)

