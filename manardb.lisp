(ql:quickload :manardb)
;;(use-package 'manardb)
(manardb:use-mmap-dir "~/metis/man-db/")

(manardb:defmmclass person ()
  ((name :type STRING :initarg :name)))

(make-instance 'person :name "John")
(print (length (manardb:retrieve-all-instances 'person)))
