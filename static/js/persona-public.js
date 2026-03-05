document.addEventListener("DOMContentLoaded", () => {
  const altLogin = document.getElementById("botoLoginAlt");
  const altReg = document.getElementById("botoRegistreAlt");

  if (altLogin) {
    altLogin.addEventListener("click", (e) => {
      const modal = document.getElementById("modalLogin");
      if (modal) {
        e.preventDefault();
        modal.style.display = "flex";
      }
    });
  }

  if (altReg) {
    altReg.addEventListener("click", (e) => {
      const modal = document.getElementById("modalRegistre");
      if (modal) {
        e.preventDefault();
        modal.style.display = "flex";
      }
    });
  }
});
