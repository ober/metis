(in-package :cl-user)
(make-package :hello :use '("COMMON-LISP"))
(in-package :hello)

(defun hello ()
  (format t "hello world~%"))
