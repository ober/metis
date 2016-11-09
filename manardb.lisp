(ql:quickload :manardb)
(manardb:use-mmap-dir "/tmp/")

(manardb:defmmclass person ()
  ((name :type STRING :initarg :name)))

(make-instance 'person :name "John")
(print (manardb:retrieve-all-instances 'person))
