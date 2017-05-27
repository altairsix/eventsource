package sqlstore

// Option represents a functional configuration of *Store
type Option func(*Store)

func WithAccessor(accessor Accessor) Option {
	return func(s *Store) {
		s.accessor = accessor
	}
}
