import ctypes
import os
from pathlib import Path
import unittest


class EarlyBootstrapRegistryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lib = ctypes.CDLL(
            os.environ.get(
                "NCCL_INIT_TRACE_LIBRARY",
                str(Path(__file__).resolve().parents[1] / "libnccl-init-trace.so"),
            )
        )
        cls.lib.nitEarlyBootstrapPublish.argtypes = [
            ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_char_p,
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
        ]
        cls.lib.nitEarlyBootstrapTake.argtypes = [
            ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_int),
        ]
        cls.lib.nitEarlyBootstrapWithdraw.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_int),
        ]

    def take(self, uid, rank=0, size=2, name=b"MRC"):
        send, recv, found = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_int()
        result = self.lib.nitEarlyBootstrapTake(
            uid, rank, size, name, ctypes.byref(send), ctypes.byref(recv), ctypes.byref(found)
        )
        return result, found.value, send.value, recv.value

    def test_exact_identity_and_single_consumption(self):
        uid = b"a" * 128
        self.assertEqual(self.lib.nitEarlyBootstrapPublish(uid, 0, 2, b"MRC", 11, 12, 13), 0)
        try:
            self.assertEqual(self.take(b"b" + uid[1:])[:2], (0, 0))
            self.assertEqual(self.take(uid, rank=1)[:2], (0, 0))
            self.assertEqual(self.take(uid, size=3)[:2], (0, 0))
            self.assertNotEqual(self.take(uid, name=b"IB")[0], 0)
            self.assertEqual(self.take(uid), (0, 1, 11, 12))
            self.assertNotEqual(self.take(uid)[0], 0)
        finally:
            consumed = ctypes.c_int()
            self.assertEqual(self.lib.nitEarlyBootstrapWithdraw(13, ctypes.byref(consumed)), 0)
            self.assertEqual(consumed.value, 1)

    def test_cancellation_prevents_later_adoption_and_allows_new_generation(self):
        uid = b"c" * 128
        for owner in (21, 22):
            self.assertEqual(self.lib.nitEarlyBootstrapPublish(uid, 0, 2, b"MRC", 11, 12, owner), 0)
            self.assertNotEqual(self.lib.nitEarlyBootstrapPublish(uid, 0, 2, b"MRC", 11, 12, 99), 0)
            consumed = ctypes.c_int(-1)
            self.assertEqual(self.lib.nitEarlyBootstrapWithdraw(owner, ctypes.byref(consumed)), 0)
            self.assertEqual(consumed.value, 0)
            self.assertEqual(self.take(uid)[:2], (0, 0))


if __name__ == "__main__":
    unittest.main()
