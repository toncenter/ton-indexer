import datetime as dt
from fractions import Fraction
import unittest

import gramstat_tvl as tvl


class TVLTests(unittest.TestCase):
    def test_native_is_a_subset_and_unpriced_is_not_zero(self):
        states = {'both': ('stonfi', ('', 'j'), (10**23+1, 3)),
                  'one': ('stonfi', ('j','x'), (1, 7)),
                  'none': ('dedust', ('unknown','x'), (4,5)),
                  'zero_side': ('dedust', ('','x'), (20,0))}
        counts, _ = tvl.value_pools(states, {'j':Fraction(5,2)})
        self.assertEqual(dict(counts['stonfi']), {'full':1,'full_nano':10**23+9,
                         'partial':1,'partial_nano':3,'native_nano':10**23+1})
        self.assertEqual(counts['dedust']['none'],1)
        self.assertEqual(counts['dedust']['full'],1)
        row = tvl.projection(dt.date(2026,11,22),123,counts)
        self.assertEqual(row['amount_nanoton'],str(10**23+32))
        self.assertEqual(row['is_partial'],1)
        counts,_ = tvl.value_pools({'a':states['none']},{})
        self.assertIsNone(tvl.projection(dt.date(2026,11,22),123,counts)['amount_nanoton'])

    def test_increment_retires_pools_but_preserves_idle_reserves(self):
        class DB:
            def execute(self, query, params):
                if 'pool_daily' in query:
                    return [('old',(2<<16, True, 1,2)),('new',(3<<16,False,4,5))]
                return [('old',('stonfi','x','y',True,1,2,2<<16)),('new',('dedust','','y',False,4,5,3<<16))]
        states={'old':('stonfi',('x','y'),(1,2)), 'idle':('dedust',('','b'),(5,6))}
        self.assertEqual(tvl.advance(DB(),dt.date(2026,11,22),states,1),3)
        self.assertEqual(set(states),{'idle','new'})
        self.assertEqual(states['idle'][2],(5,6))


if __name__ == '__main__':
    unittest.main()
