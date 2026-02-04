package core

func (a *App) resolveCognomCanonicalID(id int) (int, bool, error) {
	if id <= 0 {
		return 0, false, nil
	}
	current := id
	seen := map[int]struct{}{}
	for i := 0; i < 20; i++ {
		if _, ok := seen[current]; ok {
			break
		}
		seen[current] = struct{}{}
		redirect, err := a.DB.GetCognomRedirect(current)
		if err != nil {
			return current, current != id, err
		}
		if redirect == nil || redirect.ToCognomID <= 0 || redirect.ToCognomID == current {
			break
		}
		current = redirect.ToCognomID
	}
	return current, current != id, nil
}
