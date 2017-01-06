(defun hello () (format t "hello world~%"))(sb-ext:save-lisp-and-die "hw" :compression 5 :executable t :toplevel 'hello :save-runtime-options t)
